import os
import time
import logging
import hashlib
import json
import socket
import sys
import signal
import threading
import requests
import psycopg
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI")

# Bitcoin Core (For Funding Data)
BTC_RPC_URL = f"http://{os.getenv('BITCOIN_RPCUSER')}:{os.getenv('BITCOIN_RPCPASSWORD')}@{os.getenv('BITCOIN_RPCHOST')}:{os.getenv('BITCOIN_RPCPORT')}"

# Fulcrum (For Closure Detection)
FULCRUM_HOST = os.getenv("FULCRUM_HOST", "127.0.0.1")
FULCRUM_PORT = int(os.getenv("FULCRUM_PORT", "50001"))

BATCH_SIZE = 50

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
def decode_scid(scid_int):
    """Converts BigInt SCID to (Block, Tx, Out) for Funding Lookups"""
    if not scid_int: return None
    block = scid_int >> 40
    tx = (scid_int >> 16) & 0xFFFFFF
    out = scid_int & 0xFFFF
    return block, tx, out

def get_p2wsh_scripthash(key1_hex: str, key2_hex: str) -> str:
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
            response = requests.post(BTC_RPC_URL, json=payload, timeout=30)
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
                # Short timeout for connect, longer for data
                s = socket.create_connection((self.host, self.port), timeout=5)
                payload = {"id": 1, "jsonrpc": "2.0", "method": method, "params": params}
                s.sendall(json.dumps(payload).encode() + b'\n')
                
                # Fulcrum responses can be large (history), read until newline
                # In strict JSON-RPC over TCP, usually newline delimited.
                # A simple huge buffer read is often safer for simple clients.
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


# --- WORKER THREADS ---

def funding_worker():
    """
    Worker 1: Funding Enrichment
    Finds channels with missing capacity/timestamp and queries Bitcoin Core.
    """
    logger.info("--- Funding Worker Started ---")
    
    # Independent DB connection for this thread
    try:
        conn = psycopg.connect(POSTGRES_URI, autocommit=True)
        logger.info("--- Connected to PostgreSQL ---")
    except Exception as e:
        logger.critical(f"Funding Worker DB Connect Failed: {e}")
        return

    while True:
        try:
            # 1. Check for work
            with conn.cursor() as cur:
                cur.execute(f"SELECT gossip_id, scid FROM channels WHERE capacity_sat IS NULL LIMIT {BATCH_SIZE}")
                rows = cur.fetchall()
            
            PENDING_FUNDING.set(len(rows))

            if not rows:
                time.sleep(5)
                continue

            # 2. Prepare Data
            scid_map = {} 
            block_heights = []

            for i, (gossip_id, scid_int) in enumerate(rows):
                if not scid_int: continue
                b, t, o = decode_scid(scid_int)
                scid_map[i] = (gossip_id, b, t, o)
                block_heights.append(b)

            if not block_heights: continue

            # 3. Execute RPC Chain
            # A. Get Block Hashes
            hashes_resp = rpc_batch_request("getblockhash", block_heights)
            if not hashes_resp: continue

            # B. Get Block Data (to find TXID)
            block_hashes = [r['result'] for r in hashes_resp if 'result' in r]
            blocks_resp = rpc_batch_request("getblock", [[h, 1] for h in block_hashes])
            
            # C. Get Raw Transactions
            tx_requests = []
            valid_indices = []

            for i, block_res in enumerate(blocks_resp):
                if 'error' in block_res and block_res['error']: continue
                
                if i not in scid_map: continue # Should not happen if sync
                _, _, tx_idx, _ = scid_map[i]
                
                try:
                    block = block_res['result']
                    txid = block['tx'][tx_idx]
                    tx_requests.append([txid, True])
                    valid_indices.append(i)
                except (IndexError, KeyError):
                    logger.warning(f"Failed to find tx index {tx_idx} in block")

            txs_resp = rpc_batch_request("getrawtransaction", tx_requests)

            # 4. Update Database
            updates = 0
            with conn.cursor() as cur:
                for i, tx_res in enumerate(txs_resp):
                    if 'result' not in tx_res: continue
                    
                    original_idx = valid_indices[i]
                    gossip_id, _, _, out_idx = scid_map[original_idx]
                    
                    tx = tx_res['result']
                    try:
                        # Extract Capacity (BTC -> Sats)
                        amount_btc = tx['vout'][out_idx]['value']
                        capacity_sat = int(amount_btc * 100_000_000)
                        
                        # Extract Timestamp
                        block_time = datetime.fromtimestamp(tx['blocktime'], timezone.utc)

                        # Update DB
                        cur.execute("""
                            UPDATE channels 
                            SET funding_timestamp = %s, capacity_sat = %s 
                            WHERE gossip_id = %s
                        """, (block_time, capacity_sat, gossip_id))
                        updates += 1
                    except Exception as e:
                        logger.error(f"Parsing Error for {gossip_id}: {e}")

            logger.info(f"Enriched {updates} funding transactions")
            ENRICHED_TOTAL.inc(updates)

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