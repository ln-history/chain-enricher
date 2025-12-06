import os
import time
import logging
import requests
import json
import sys
import signal
import psycopg
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI")
BTC_RPC_URL = f"http://{os.getenv('BITCOIN_RPCUSER')}:{os.getenv('BITCOIN_RPCPASSWORD')}@{os.getenv('BITCOIN_RPCHOST')}:{os.getenv('BITCOIN_RPCPORT')}"
BATCH_SIZE = 50  # Number of channels to process per loop

# --- LOGGING SETUP ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("chain-enricher")

# --- METRICS SETUP ---
ENRICHED_TOTAL = Counter('chain_enriched_channels_total', 'Total channels enriched with chain data')
PENDING_GAUGE = Gauge('chain_pending_channels', 'Number of channels waiting for enrichment')
RPC_LATENCY = Histogram('chain_rpc_latency_seconds', 'Time spent waiting for Bitcoin Core RPC')
RPC_ERRORS = Counter('chain_rpc_errors_total', 'Total failures calling Bitcoin RPC')

def decode_scid(scid_int):
    """Converts BigInt SCID to (Block, Tx, Out)"""
    if not scid_int: return None
    block = scid_int >> 40
    tx = (scid_int >> 16) & 0xFFFFFF
    out = scid_int & 0xFFFF
    return block, tx, out

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

def run_enrichment_loop():
    # Connect to DB
    try:
        conn = psycopg.connect(POSTGRES_URI, autocommit=True)
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.critical(f"Failed to connect to DB: {e}")
        sys.exit(1)

    while True:
        try:
            # 1. Check for work
            with conn.cursor() as cur:
                cur.execute(f"SELECT gossip_id, scid FROM channels WHERE capacity_sat IS NULL LIMIT {BATCH_SIZE}")
                rows = cur.fetchall()
            
            PENDING_GAUGE.set(len(rows)) # Update metric

            if not rows:
                time.sleep(5) # Sleep if no work
                continue

            logger.info(f"⚡ Processing batch of {len(rows)} channels...")

            # 2. Prepare Data for RPC
            scid_map = {} # Map ID -> (gossip_id, block_height, tx_idx, out_idx)
            block_heights = []

            for gossip_id, scid_int in rows:
                if not scid_int: continue
                b, t, o = decode_scid(scid_int)
                scid_map[len(block_heights)] = (gossip_id, b, t, o)
                block_heights.append(b)

            # 3. Execute RPC Chain (Batch 1: Get Hashes)
            # We ask for block hashes for all heights
            hashes_resp = rpc_batch_request("getblockhash", block_heights)
            if not hashes_resp:
                time.sleep(5); continue

            # 4. Execute RPC Chain (Batch 2: Get Block Data)
            # We get block data (verbosity=1) to find TXIDs
            block_hashes = [r['result'] for r in hashes_resp]
            blocks_resp = rpc_batch_request("getblock", [[h, 1] for h in block_hashes])
            
            # 5. Execute RPC Chain (Batch 3: Get Raw Transaction)
            # We extract the specific TXID we need from the block
            tx_requests = []
            valid_indices = []

            for i, block_res in enumerate(blocks_resp):
                if 'error' in block_res and block_res['error']:
                    logger.warning(f"Error fetching block: {block_res['error']}")
                    continue
                
                gossip_id, _, tx_idx, _ = scid_map[i]
                block = block_res['result']
                
                try:
                    txid = block['tx'][tx_idx] # Get the specific transaction ID
                    tx_requests.append([txid, True]) # True = verbose (decode)
                    valid_indices.append(i) # Keep track of which original request this matches
                except IndexError:
                    logger.error(f"Transaction index {tx_idx} out of bounds for block {block['hash']}")

            txs_resp = rpc_batch_request("getrawtransaction", tx_requests)

            # 6. Update Database
            updates = 0
            with conn.cursor() as cur:
                for i, tx_res in enumerate(txs_resp):
                    original_index = valid_indices[i]
                    gossip_id, _, _, out_idx = scid_map[original_index]

                    if 'result' not in tx_res: continue
                    
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
                        logger.error(f"Failed to parse TX for gossip_id {gossip_id}: {e}")

            logger.info(f"✅ Enriched {updates} channels successfully.")
            ENRICHED_TOTAL.inc(updates)

        except Exception as e:
            logger.error(f"Loop failed: {e}")
            RPC_ERRORS.inc()
            time.sleep(5)

if __name__ == "__main__":
    # Start Prometheus metrics server on different port (8001)
    start_http_server(8001)
    logger.info("📈 Metrics server started on port 8001")
    
    # Handle shutdown
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    
    run_enrichment_loop()