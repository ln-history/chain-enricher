import os
import time
import logging
import hashlib
import json
import socket
import sys
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple
import requests
import psycopg
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://user:secret@ln-history-database/lnhistory")

# Bitcoin Core (For Funding Data)
BTC_RPC_URL = f"http://{os.getenv('BITCOIN_RPCUSER')}:{os.getenv('BITCOIN_RPCPASSWORD')}@{os.getenv('BITCOIN_RPCHOST')}:{os.getenv('BITCOIN_RPCPORT')}"

# Fulcrum (For Closure Detection)
FULCRUM_HOST = os.getenv("FULCRUM_HOST", "127.0.0.1")
FULCRUM_PORT = int(os.getenv("FULCRUM_PORT", "50001"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
FUNDING_WORKERS = int(os.getenv("FUNDING_WORKERS", str(BATCH_SIZE//5)))
RPC_TIMEOUT_SECONDS = int(os.getenv("RPC_TIMEOUT_SECONDS", 60))

# --- LOGGING ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("chain-enricher")

# --- METRICS ---
# Funding Metrics
ENRICHED_TOTAL = Counter('chain_enriched_channels_total', 'Total channels enriched with funding data')
PENDING_FUNDING = Gauge('chain_pending_funding', 'Number of channels waiting for funding data')
RPC_LATENCY = Histogram('chain_rpc_latency_seconds', 'Time spent waiting for Bitcoin Core RPC')
RPC_ERRORS = Counter('chain_rpc_errors_total', 'Total failures calling Bitcoin RPC')

# Closure Metrics
CLOSURE_CHECK_TOTAL = Counter('chain_closure_checks_total', 'Total closure checks performed')
CLOSURES_DETECTED = Counter('chain_closures_detected_total', 'Total closed channels found', ['type'])
OPEN_CHANNELS_GAUGE = Gauge('lightning_open_channels_count', 'Currently open channels in DB')
FULCRUM_LATENCY = Histogram('chain_fulcrum_latency_seconds', 'Fulcrum RPC latency')


# --- HELPER FUNCTIONS ---
def decode_scid(scid_int: int) -> Optional[Tuple[int, int, int]]:
    """Converts BigInt SCID to (Block, Tx, Out) for Funding Lookups"""
    if not scid_int: return None
    block = scid_int >> 40
    tx = (scid_int >> 16) & 0xFFFFFF
    out = scid_int & 0xFFFF
    return block, tx, out

def get_p2wsh_scripthash(key1_hex: str, key2_hex: str) -> Optional[str]:
    """Derives the Electrum ScriptHash for a 2-of-2 Lightning Multisig"""
    try:
        pk_bytes = [bytes.fromhex(key1_hex), bytes.fromhex(key2_hex)]
        pk_bytes.sort() # BIP67: Keys must be sorted lexicographically
        
        # Witness Script: OP_2 <pk1> <pk2> OP_2 OP_CHECKMULTISIG
        # OP_2=0x52, Push33=0x21, OP_CHECKMULTISIG=0xae
        witness_script = b'\x52\x21' + pk_bytes[0] + b'\x21' + pk_bytes[1] + b'\x52\xae'
        
        # P2WSH ScriptPubKey: OP_0 <sha256(witness)>
        sha256_witness = hashlib.sha256(witness_script).digest()
        script_pubkey = b'\x00\x20' + sha256_witness
        
        # Electrum Hash: sha256(script_pubkey) reversed
        return hashlib.sha256(script_pubkey).digest()[::-1].hex()
    except Exception:
        return None

# --- CLIENTS ---

def rpc_batch_request(method, params_list):
    """Sends a batched JSON-RPC request to Bitcoin Core"""
    payload = []
    for i, params in enumerate(params_list):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": method,
            "params": params if isinstance(params, list) else [params]
        })
    
    with RPC_LATENCY.time():
        try:
            response = requests.post(BTC_RPC_URL, json=payload, timeout=RPC_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"RPC Batch Failed: {e}")
            RPC_ERRORS.inc()
            return None

class FulcrumClient:
    """Simple blocking socket client for Fulcrum JSON-RPC"""
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    def call(self, method, params):
        with FULCRUM_LATENCY.time():
            try:
                s = socket.create_connection((self.host, self.port), timeout=30)
                payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}
                s.sendall(json.dumps(payload).encode() + b'\n')
                data = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk: break
                    data += chunk
                    if b'\n' in chunk: break
                s.close()
                resp = json.loads(data.decode())
                if 'error' in resp: raise Exception(resp['error'])
                return resp['result']
            except Exception as e:
                logger.error(f"Fulcrum Error: {e}")
                return None


# --- FUNDING HELPERS ---

def _fetch_funding_data(args: tuple) -> Optional[Tuple[str, int, datetime]]:
    """
    Fetch funding capacity and timestamp for one channel via Fulcrum.
    Called concurrently from funding_worker's fast path.
    Returns (gossip_id, capacity_sat, funding_timestamp) or None on failure.
    """
    gid, out_idx, key1, key2 = args
    scripthash = get_p2wsh_scripthash(key1, key2)
    if not scripthash:
        return None

    fulcrum = FulcrumClient(FULCRUM_HOST, FULCRUM_PORT)

    history = fulcrum.call('blockchain.scripthash.get_history', [scripthash])
    if not history:
        return None

    # Earliest confirmed entry is the funding tx
    history.sort(key=lambda x: x.get('height', 0))
    funding_entry = history[0]
    if funding_entry.get('height', 0) <= 0:
        return None  # Unconfirmed — retry later

    raw_tx = fulcrum.call('blockchain.transaction.get', [funding_entry['tx_hash'], True])
    if not raw_tx:
        return None

    try:
        capacity_sat = int(raw_tx['vout'][out_idx]['value'] * 100_000_000)
        block_time = datetime.fromtimestamp(raw_tx['blocktime'], timezone.utc)
        return gid, capacity_sat, block_time
    except Exception as e:
        logger.error(f"Parsing Error for {gid}: {e}")
        return None


def _enrich_via_btc_core(conn: psycopg.Connection, rows: list) -> int:
    """
    Enrich channels that lack bitcoin keys using the Bitcoin Core RPC chain.
    Returns number of channels updated.
    """
    channels = []
    block_heights = []
    for gossip_id, scid_int in rows:
        coords = decode_scid(scid_int)
        if coords is None:
            continue
        b, t, o = coords
        channels.append((gossip_id, b, t, o))
        block_heights.append(b)

    if not block_heights:
        return 0

    hashes_resp = rpc_batch_request("getblockhash", block_heights)
    if not hashes_resp:
        return 0

    block_requests = []
    block_channel_idx: list[int] = []
    for j, r in enumerate(hashes_resp):
        if 'result' in r:
            block_requests.append([r['result'], 1])
            block_channel_idx.append(j)

    if not block_requests:
        return 0

    blocks_resp = rpc_batch_request("getblock", block_requests)
    if not blocks_resp:
        return 0

    tx_requests = []
    tx_channel_idx: list[int] = []
    for k, block_res in enumerate(blocks_resp):
        if block_res.get('error') or 'result' not in block_res:
            continue
        j = block_channel_idx[k]
        _, _, tx_idx, _ = channels[j]
        try:
            txid = block_res['result']['tx'][tx_idx]
            tx_requests.append([txid, True])
            tx_channel_idx.append(j)
        except (IndexError, KeyError):
            logger.warning(f"Failed to find tx index {tx_idx} in block for {channels[j][0]}")

    if not tx_requests:
        return 0

    txs_resp = rpc_batch_request("getrawtransaction", tx_requests)
    if not txs_resp:
        return 0

    updates = 0
    with conn.cursor() as cur:
        for m, tx_res in enumerate(txs_resp):
            if 'result' not in tx_res:
                continue
            j = tx_channel_idx[m]
            gossip_id, _, _, out_idx = channels[j]
            tx = tx_res['result']
            try:
                capacity_sat = int(tx['vout'][out_idx]['value'] * 100_000_000)
                block_time = datetime.fromtimestamp(tx['blocktime'], timezone.utc)
                cur.execute("""
                    UPDATE channels
                    SET funding_timestamp = %s, capacity_sat = %s
                    WHERE gossip_id = %s
                """, (block_time, capacity_sat, gossip_id))
                updates += 1
            except Exception as e:
                logger.error(f"Parsing Error for {gossip_id}: {e}")
    return updates


# --- WORKER THREADS ---

def funding_worker():
    """
    Worker 1: Funding Enrichment

    Fast path  — channels with bitcoin_key_1/2: derives the P2WSH scripthash and
    queries Fulcrum directly, skipping the expensive getblock call entirely.
    All channels in the batch are queried concurrently via a thread pool.

    Slow path  — channels without keys: falls back to the Bitcoin Core RPC chain
    (getblockhash → getblock → getrawtransaction).
    """
    logger.info("--- Funding Worker Started ---")

    try:
        conn = psycopg.connect(POSTGRES_URI, autocommit=True)
        logger.info("--- Connected to PostgreSQL ---")
    except Exception as e:
        logger.critical(f"Funding Worker DB Connect Failed: {e}")
        return

    while True:
        try:
            did_work = False

            # === FAST PATH: Fulcrum (channels with both bitcoin keys) ===
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT gossip_id, scid, bitcoin_key_1, bitcoin_key_2
                    FROM channels
                    WHERE (capacity_sat IS NULL OR funding_timestamp IS NULL)
                      AND scid IS NOT NULL
                      AND bitcoin_key_1 IS NOT NULL
                      AND bitcoin_key_2 IS NOT NULL
                    LIMIT {BATCH_SIZE}
                """) # type: ignore
                fulcrum_rows = cur.fetchall()

            if fulcrum_rows:
                PENDING_FUNDING.set(len(fulcrum_rows))
                args_list = []
                for gid, scid_int, k1, k2 in fulcrum_rows:
                    coords = decode_scid(scid_int)
                    if coords is None:
                        continue
                    _, _, out_idx = coords
                    args_list.append((gid, out_idx, k1, k2))

                with ThreadPoolExecutor(max_workers=FUNDING_WORKERS) as executor:
                    results = list(executor.map(_fetch_funding_data, args_list))

                updates = 0
                with conn.cursor() as cur:
                    for result in results:
                        if result is None:
                            continue
                        gid, capacity_sat, block_time = result
                        cur.execute("""
                            UPDATE channels
                            SET funding_timestamp = %s, capacity_sat = %s
                            WHERE gossip_id = %s
                        """, (block_time, capacity_sat, gid))
                        updates += 1

                logger.info(f"Enriched {updates}/{len(fulcrum_rows)} channels via Fulcrum")
                ENRICHED_TOTAL.inc(updates)
                did_work = True

            # === SLOW PATH: Bitcoin Core (channels missing one or both keys) ===
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT gossip_id, scid
                    FROM channels
                    WHERE (capacity_sat IS NULL OR funding_timestamp IS NULL)
                      AND scid IS NOT NULL
                      AND (bitcoin_key_1 IS NULL OR bitcoin_key_2 IS NULL)
                    LIMIT {BATCH_SIZE}
                """) # type: ignore
                btc_rows = cur.fetchall()

            if btc_rows:
                updates = _enrich_via_btc_core(conn, btc_rows)
                logger.info(f"Enriched {updates}/{len(btc_rows)} channels via Bitcoin Core")
                ENRICHED_TOTAL.inc(updates)
                did_work = True

            if not did_work:
                time.sleep(5)

        except Exception as e:
            logger.error(f"Funding Loop Error: {e}")
            RPC_ERRORS.inc()
            time.sleep(5)


def closure_worker():
    """
    Worker 2: Closure Detection
    Scans open channels via Fulcrum to detect spends and classify closures.
    """
    logger.info("--- Closure Worker Started ---")
    
    try:
        conn = psycopg.connect(POSTGRES_URI, autocommit=True)
    except Exception as e:
        logger.critical(f"Closure Worker DB Connect Failed: {e}")
        return

    fulcrum = FulcrumClient(FULCRUM_HOST, FULCRUM_PORT)

    while True:
        try:
            # 1. Get Open Channels
            with conn.cursor() as cur:
                # Update Metric
                cur.execute("SELECT count(*) FROM channels WHERE closing_timestamp IS NULL")
                count = cur.fetchone()[0]
                OPEN_CHANNELS_GAUGE.set(count)

                # Fetch Random Batch of OPEN channels that have KEYS and CAPACITY
                # (Capacity needed for fee calc, Keys needed for script derivation)
                cur.execute("""
                    SELECT gossip_id, bitcoin_key_1, bitcoin_key_2, capacity_sat 
                    FROM channels 
                    WHERE closing_timestamp IS NULL 
                      AND bitcoin_key_1 IS NOT NULL 
                      AND bitcoin_key_2 IS NOT NULL
                      AND capacity_sat IS NOT NULL
                    ORDER BY RANDOM() LIMIT 20
                """)
                rows = cur.fetchall()

            if not rows:
                time.sleep(10)
                continue

            for gid, k1, k2, capacity_sat in rows:
                # Derive P2WSH Address (ScriptHash)
                scripthash = get_p2wsh_scripthash(k1, k2)
                if not scripthash: continue

                # Query Fulcrum History
                history = fulcrum.call('blockchain.scripthash.get_history', [scripthash])
                
                # Logic: A channel usually has 1 tx (Funding). If 2+, it's closed (or spliced).
                if not history or len(history) < 2:
                    continue 

                # Channel is CLOSED. Find the spending transaction.
                # Sort by height to find the latest event.
                history.sort(key=lambda x: x['height'])
                
                # The last event is likely the closure
                closing_event = history[-1]
                closing_txid = closing_event['tx_hash']
                closing_height = closing_event['height']

                if closing_height <= 0: 
                    # Mempool transaction. Valid, but be careful with timestamps.
                    # We can process it, or wait for confirm. Let's process.
                    pass

                # Fetch Full Closing TX (Verbose)
                raw_tx = fulcrum.call('blockchain.transaction.get', [closing_txid, True])
                if not raw_tx: continue

                # --- ANALYSIS ---

                # 1. Timestamp
                if 'blocktime' in raw_tx:
                    close_ts = datetime.fromtimestamp(raw_tx['blocktime'], timezone.utc)
                else:
                    # Fallback for mempool or missing blocktime
                    if closing_height > 0:
                        header = fulcrum.call('blockchain.block.header', [closing_height])
                        # Timestamp is at offset 68 (4 bytes LE) in standard header
                        ts_int = int.from_bytes(bytes.fromhex(header)[68:72], 'little')
                        close_ts = datetime.fromtimestamp(ts_int, timezone.utc)
                    else:
                        close_ts = datetime.now(timezone.utc)

                # 2. Financials
                outputs = raw_tx.get('vout', [])
                total_out_sat = sum(int(o['value'] * 100_000_000) for o in outputs)
                
                # Fee = Input (Capacity) - Outputs
                mining_fee = max(0, capacity_sat - total_out_sat)
                
                # Balance Distribution (Heuristic: Take 2 largest outputs)
                # Ignore dust (< 546 sats) often used for anchors
                significant_outs = sorted(
                    [int(o['value']*100_000_000) for o in outputs if o['value'] > 0.00000546],
                    reverse=True
                )
                
                bal_1 = significant_outs[0] if len(significant_outs) > 0 else 0
                bal_2 = significant_outs[1] if len(significant_outs) > 1 else 0

                # 3. Type Detection (RFC Precise)
                closure_type = 'unknown'
                inputs = raw_tx.get('vin', [])
                
                if inputs:
                    spending_input = inputs[0] # Channels usually spent by input 0
                    sequence = spending_input.get('sequence', 0)
                    
                    # --- SEQUENCE CHECK (RFC Bolt 3) ---
                    # Mutual Close: Sequence is 0xFFFFFFFF or 0xFFFFFFFE (RBF)
                    # Force Close: Sequence encodes commitment number, always < 0xFFFFFFFE
                    is_sequence_final = sequence >= 0xFFFFFFFE
                    
                    if is_sequence_final:
                        closure_type = 'mutual'
                    else:
                        closure_type = 'force'
                        
                    # Note: "Breach" looks like "Force" here. 
                    # To detect breach, we'd need to watch if this output gets swept by a justice key.

                # --- DB UPDATE ---
                with conn.cursor() as cur:
                    # 1. Mark Channel as Closed
                    cur.execute("""
                        UPDATE channels SET closing_timestamp = %s 
                        WHERE gossip_id = %s
                    """, (close_ts, gid))
                    
                    # 2. Insert Analysis
                    cur.execute("""
                        INSERT INTO channel_closures 
                        (gossip_id, closing_txid, closing_height, closing_timestamp, type, 
                         settled_balance_sat, mining_fee_sat, 
                         output_0_sat, output_1_sat, balance_node_1_sat, balance_node_2_sat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (gossip_id) DO NOTHING
                    """, (
                        gid, closing_txid, closing_height, close_ts, closure_type,
                        total_out_sat, mining_fee,
                        bal_1, bal_2, bal_1, bal_2 
                    ))

                logger.info(f"CLOSED: {gid} | Type: {closure_type} | Fee: {mining_fee} sat")
                CLOSURES_DETECTED.labels(type=closure_type).inc()
                
            CLOSURE_CHECK_TOTAL.inc(len(rows))

        except Exception as e:
            logger.error(f"Closure Loop Error: {e}")
            time.sleep(5)


# --- MAIN ---

if __name__ == "__main__":
    # Start Metrics Server
    start_http_server(8001)
    logger.info("--- Metrics server started on port 8001 ---")
    
    # Start Workers
    t1 = threading.Thread(target=funding_worker, daemon=True)
    t2 = threading.Thread(target=closure_worker, daemon=True)
    
    t1.start()
    t2.start()
    
    # Graceful Shutdown
    def handle_exit(signum, frame):
        logger.info("Shutting down enricher service...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Keep Main Thread Alive
    while True:
        time.sleep(1)