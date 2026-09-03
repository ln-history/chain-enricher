"""Backfill channels.funding_txid and channel_funding_inputs for existing channels.

WHY THIS EXISTS
    chain-enricher records funding provenance from 2026-09-03 onward. The 510,117
    channels enriched before that have capacity_sat and funding_timestamp but no record
    of WHICH transaction funded them or WHAT it spent -- and the funding inputs are the
    only on-chain evidence of who opened a channel. BOLT 7 orders node_id_1/node_id_2
    lexicographically and deliberately says nothing about who funded.

THE SCID IS THE AUTHORITY
    A channel's short_channel_id IS its funding outpoint:

        block_height << 40 | tx_index << 16 | output_index

    so the funding transaction is "the tx_index'th transaction of block block_height",
    full stop. This resolves that position rather than searching for a plausible
    transaction. The alternative -- Fulcrum's scripthash history for the channel's P2WSH,
    which is what the enricher's fast path uses -- answers a different question ("what
    paid this script?") and can only agree by coincidence when a script is reused.

EVERY ROW IS VERIFIED BEFORE IT IS WRITTEN
    The resolved transaction must have an output at output_index whose value equals the
    capacity_sat already stored for the channel. That value was derived independently, by
    an earlier enricher run, often via the Fulcrum path -- so agreement is a genuine
    cross-check that the scid decoded correctly and that Fulcrum's position index and
    Core's block agree. A mismatch is REFUSED and counted, never written: a wrong
    funding_txid would silently corrupt every attribution built on top of it.

    ONE DISAGREEMENT IS EXPECTED AND IS THE STORED VALUE'S FAULT.
    4.75% of channels store a capacity exactly 1 sat below the chain -- 190 of the first
    4,000, every one of them off by exactly +1, against a round on-chain number
    (14,999 vs 15,000; 119,999 vs 120,000). That is the float truncation btc_to_sat
    exists to fix: 0.00015 is not representable in binary, so int(0.00015 * 100_000_000)
    is 14999. Truncation can only ever round DOWN, so chain = stored + 1 is that bug and
    chain = stored - 1 could not be. The first is accepted and the stored capacity is
    REPAIRED from the chain; anything else is refused.

SHAPE
    Fulcrum resolves (block, tx_index) -> txid in ~1.3 ms via its position index, which
    avoids reading 208,106 whole blocks to find 510,117 transactions. Bitcoin Core then
    serves the transactions themselves. Core is remote here (Tailscale), and measurement
    says its disk, not the link, is the limit: 8 workers x 50-tx batches ran at 11 ms/tx
    while 24 workers ran at 45 ms/tx. Queueing deeper makes it slower, so the defaults
    are deliberately modest.

    Resumable and idempotent. Progress is the data itself -- a channel with a
    funding_txid is skipped -- so an interrupted run is resumed by re-running it.

Usage:
    docker run --rm --network ln-history-network \
        --env-file ~/ln-history-research/.env \
        -e POSTGRES_URI="postgresql://admin:...@ln-history-database:5432/lnhistory" \
        -v ~/ln-history-research/chain-enricher/migrations:/m \
        ghcr.io/ln-history/chain-enricher:0.6.3 python /m/backfill_funding_provenance.py [--apply]
"""

import argparse
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg
import requests

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://user:secret@ln-history-database/lnhistory")
BTC_RPC_URL = (
    f"http://{os.getenv('BITCOIN_RPCUSER')}:{os.getenv('BITCOIN_RPCPASSWORD')}"
    f"@{os.getenv('BITCOIN_RPCHOST')}:{os.getenv('BITCOIN_RPCPORT', '8332')}"
)
FULCRUM_HOST = os.getenv("FULCRUM_HOST", os.getenv("BITCOIN_RPCHOST", "127.0.0.1"))
FULCRUM_PORT = int(os.getenv("FULCRUM_PORT", "50001"))

#: Channels resolved per round trip to the database.
PAGE = 2_000
#: Concurrent Bitcoin Core connections, and transactions per JSON-RPC batch. Measured on
#: this node: (8, 50) -> 11 ms/tx; (24, 50) -> 45 ms/tx. Deeper is slower, not faster.
WORKERS = int(os.getenv("BACKFILL_WORKERS", "8"))
TX_BATCH = int(os.getenv("BACKFILL_TX_BATCH", "50"))
#: Fulcrum applies a server-side deadline per batch and drops the connection when it
#: trips; 200-entry transaction batches reliably tripped it, position lookups do not.
FULCRUM_BATCH = 100


def btc_to_sat(value) -> int:
    """Convert a Core BTC amount to satoshis exactly.

    Core sends amounts as JSON numbers, so ``int(value * 100_000_000)`` truncates on the
    wrong side of a binary rounding error -- a 105,539,536 sat output became
    105,539,535 in production. Going through Decimal on the string form keeps the
    decimal value the node actually sent.
    """
    return int(Decimal(str(value)).scaleb(8).to_integral_value(rounding=ROUND_HALF_UP))


def decode_scid(scid: int) -> Tuple[int, int, int]:
    """(block_height, tx_index, output_index) -- the funding outpoint the scid names."""
    return scid >> 40, (scid >> 16) & 0xFFFFFF, scid & 0xFFFF


class Fulcrum:
    """Persistent batching client. Reconnects on any protocol-level failure.

    A single connection is reused because the handshake dominates a position lookup, but
    a failed batch desynchronises the stream -- the next read returns the tail of the
    previous response -- so a failure drops the socket rather than trying to recover it.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port
        self._file = None

    def _connect(self):
        if self._file is None:
            self._file = socket.create_connection((self.host, self.port), timeout=180).makefile("rwb")
        return self._file

    def _drop(self) -> None:
        try:
            if self._file is not None:
                self._file.close()
        except OSError:
            pass
        self._file = None

    def batch(self, method: str, params: List[list]) -> List[Optional[object]]:
        results: List[Optional[object]] = []
        for start in range(0, len(params), FULCRUM_BATCH):
            results += self._one(method, params[start : start + FULCRUM_BATCH])
        return results

    def _one(self, method: str, params: List[list]) -> List[Optional[object]]:
        out: List[Optional[object]] = [None] * len(params)
        request = [{"jsonrpc": "2.0", "id": i, "method": method, "params": p} for i, p in enumerate(params)]
        try:
            handle = self._connect()
            handle.write((json.dumps(request) + "\n").encode())
            handle.flush()
            response = json.loads(handle.readline().decode())
        except Exception as exc:  # noqa: BLE001 -- any failure means resolve these later
            print(f"  fulcrum batch failed ({exc}); {len(params)} positions deferred", file=sys.stderr)
            self._drop()
            return out
        if not isinstance(response, list):  # a bare error object, e.g. batch timeout
            print(f"  fulcrum error: {str(response)[:160]}", file=sys.stderr)
            self._drop()
            return out
        for item in response:
            if isinstance(item, dict) and isinstance(item.get("id"), int) and "result" in item:
                out[item["id"]] = item["result"]
        return out


def core_transactions(txids: List[str]) -> Dict[str, dict]:
    """Fetch verbose transactions by id, concurrently. Missing ids are simply absent."""
    batches = [txids[i : i + TX_BATCH] for i in range(0, len(txids), TX_BATCH)]

    def fetch(batch: List[str]) -> List[dict]:
        request = [
            {"jsonrpc": "2.0", "id": i, "method": "getrawtransaction", "params": [txid, True]}
            for i, txid in enumerate(batch)
        ]
        try:
            response = requests.post(BTC_RPC_URL, json=request, timeout=600)
            response.raise_for_status()
            return [item.get("result") for item in response.json() if isinstance(item, dict)]
        except Exception as exc:  # noqa: BLE001
            print(f"  core batch of {len(batch)} failed ({exc})", file=sys.stderr)
            return []

    found: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for result in pool.map(fetch, batches):
            for tx in result:
                if tx and tx.get("txid"):
                    found[tx["txid"]] = tx
    return found


def funding_inputs(tx: dict) -> List[Tuple[int, str, int]]:
    """(vin_index, prev_txid, prev_vout) in wire order; coinbase inputs are skipped."""
    inputs = []
    for index, vin in enumerate(tx.get("vin") or []):
        prev_txid, prev_vout = vin.get("txid"), vin.get("vout")
        if prev_txid is not None and prev_vout is not None:
            inputs.append((index, prev_txid, int(prev_vout)))
    return inputs


def pages(
    conn: psycopg.Connection, start_scid: int = -1, stop_scid: Optional[int] = None
) -> Iterable[List[Tuple[str, int, Optional[int]]]]:
    """Channels still missing a funding_txid, in scid order, paged by a moving cursor.

    The cursor is the scid rather than ``funding_txid IS NULL`` alone: a channel this run
    refuses to write (capacity mismatch, missing transaction) still has a NULL
    funding_txid, and re-selecting it would spin on the same rows forever.
    """
    after = start_scid
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gossip_id, scid, capacity_sat
                FROM channels
                WHERE funding_txid IS NULL AND scid IS NOT NULL AND scid > %s
                  AND (%s::bigint IS NULL OR scid <= %s::bigint)
                ORDER BY scid
                LIMIT %s
                """,
                (after, stop_scid, stop_scid, PAGE),
            )
            rows = cur.fetchall()
        if not rows:
            return
        after = rows[-1][1]
        yield rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill channel funding provenance")
    parser.add_argument("--apply", action="store_true", help="write; without it, resolve and report only")
    parser.add_argument("--limit", type=int, default=0, help="stop after this many channels (0 = all)")
    # Two processes over disjoint scid ranges finish sooner when Bitcoin Core has spare
    # IO; they cannot collide, because the ranges do not overlap and each row is claimed
    # by exactly one of them.
    parser.add_argument("--start-scid", type=int, default=-1, help="resume above this scid")
    parser.add_argument("--stop-scid", type=int, default=None, help="stop at this scid, inclusive")
    args = parser.parse_args()

    fulcrum = Fulcrum(FULCRUM_HOST, FULCRUM_PORT)
    started = time.time()
    seen = written = inputs_written = repairs = 0
    unresolved = missing_tx = mismatched = no_output = 0

    with psycopg.connect(POSTGRES_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*) FROM channels WHERE funding_txid IS NULL AND scid IS NOT NULL
                   AND scid > %s AND (%s::bigint IS NULL OR scid <= %s::bigint)""",
                (args.start_scid, args.stop_scid, args.stop_scid),
            )
            total = cur.fetchone()[0]
        print(f"{total:,} channels need funding provenance" + ("" if args.apply else "  (dry run)"))

        for page in pages(conn, args.start_scid, args.stop_scid):
            positions = [decode_scid(scid) for _, scid, _ in page]
            txids = fulcrum.batch(
                "blockchain.transaction.id_from_pos",
                [[block, tx_index, False] for block, tx_index, _ in positions],
            )
            wanted = sorted({t for t in txids if isinstance(t, str)})
            transactions = core_transactions(wanted)

            resolved: List[Tuple[str, str, List[Tuple[int, str, int]], Optional[int]]] = []
            for (gossip_id, _, capacity_sat), (_, _, out_index), txid in zip(page, positions, txids):
                seen += 1
                if not isinstance(txid, str):
                    unresolved += 1
                    continue
                tx = transactions.get(txid)
                if tx is None:
                    missing_tx += 1
                    continue
                outputs = tx.get("vout") or []
                if out_index >= len(outputs):
                    no_output += 1
                    continue
                # The cross-check: capacity_sat was derived independently by an earlier
                # enricher run. If it disagrees, the position resolved to the wrong
                # transaction and writing it would corrupt every attribution downstream.
                on_chain = btc_to_sat(outputs[out_index]["value"])
                repair = None
                if capacity_sat is not None and on_chain != capacity_sat:
                    # Exactly one satoshi low is the old int(value * 1e8) truncation, which
                    # can only round down. The transaction is right; the stored number is
                    # not, and we are holding the authoritative value.
                    if on_chain - capacity_sat == 1:
                        repair = on_chain
                    else:
                        mismatched += 1
                        continue
                resolved.append((gossip_id, txid, funding_inputs(tx), repair))

            repairs += sum(1 for *_, repair in resolved if repair is not None)
            if args.apply and resolved:
                with conn.cursor() as cur:
                    cur.executemany(
                        """UPDATE channels
                           SET funding_txid = %s, capacity_sat = COALESCE(%s, capacity_sat)
                           WHERE gossip_id = %s""",
                        [(txid, repair, gossip_id) for gossip_id, txid, _, repair in resolved],
                    )
                    rows = [
                        (gossip_id, vin_index, prev_txid, prev_vout)
                        for gossip_id, _, ins, _ in resolved
                        for vin_index, prev_txid, prev_vout in ins
                    ]
                    cur.executemany(
                        """INSERT INTO channel_funding_inputs (gossip_id, vin_index, prev_txid, prev_vout)
                           VALUES (%s, %s, %s, %s) ON CONFLICT (gossip_id, vin_index) DO NOTHING""",
                        rows,
                    )
                    inputs_written += len(rows)
                conn.commit()
            written += len(resolved)

            rate = seen / max(time.time() - started, 1e-9)
            remaining = (total - seen) / rate if rate else 0
            print(
                f"  {seen:,}/{total:,}  resolved={written:,}  {rate:.0f}/s  eta {remaining / 3600:.1f}h",
                end="\r",
                flush=True,
            )
            if args.limit and seen >= args.limit:
                break

    elapsed = time.time() - started
    print(f"\n{seen:,} examined in {elapsed / 60:.1f} min")
    print(f"  {written:,} funding transactions resolved and verified")
    if repairs:
        verb = "repaired" if args.apply else "would be repaired"
        print(f"  {repairs:,} capacity_sat values {verb} (+1 sat; old int(value * 1e8) truncation)")
    if args.apply:
        print(f"  {inputs_written:,} funding inputs recorded")
    else:
        print("  dry run; pass --apply to write")
    for label, count in (
        ("no transaction at that block position", unresolved),
        ("position resolved but Core has no such transaction", missing_tx),
        ("transaction has no output at the scid's output_index", no_output),
        ("output value disagrees with stored capacity_sat by more than the known +1 -- REFUSED", mismatched),
    ):
        if count:
            print(f"  {count:,} {label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
