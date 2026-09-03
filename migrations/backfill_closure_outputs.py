"""Backfill channel_closure_outputs for closures recorded before 2026-09-03.

WHY THIS EXISTS
    Attribution needs to know WHICH closing output funded the next channel, what it was
    worth, and what script it paid. channel_closures keeps none of that. It stores
    output_0_sat and output_1_sat, which are the largest and second-largest output BY
    VALUE -- not vout[0] and vout[1] -- after dropping anything at or below 546 sat as
    anchor dust. The sort destroys the vout order the funding-input join needs, the dust
    filter drops outputs entirely, and the scriptPubKey was never stored at all.

    None of that is recoverable from the table. It is recoverable from the chain, which
    is what this does, for the 468,176 closures that predate the enricher change.

    (balance_node_1_sat and balance_node_2_sat are written from those same two values and
    carry no node attribution whatsoever -- see the migration SQL. This script does not
    touch them.)

VERIFICATION, AND THE TWO ARITHMETICS
    Every output is summed and checked against the stored settled_balance_sat before the
    rows are written. That sum was computed by whichever enricher recorded the closure,
    and there have been two:

        old:  sum(int(value * 100_000_000))   -- truncates, once per output
        new:  sum(btc_to_sat(value))          -- exact

    Both are computed here and either one matching is accepted, which pins down the
    transaction exactly while tolerating the known bug. A closure matching only the
    legacy sum has a settled_balance_sat that is demonstrably light, and --repair rewrites
    it, along with output_0_sat / output_1_sat, from the chain.

    A closure matching neither is REFUSED: it means the stored closing_txid and the
    transaction we fetched are not the same event, and writing its outputs would put
    fictional money into the attribution graph.

Usage:
    docker run --rm --network ln-history-network --env-file ~/ln-history-research/.env \
        -e POSTGRES_URI=... -e FULCRUM_HOST=... \
        -v ~/ln-history-research/chain-enricher/migrations:/m \
        ghcr.io/ln-history/chain-enricher:0.7.0 \
        python /m/backfill_closure_outputs.py [--apply] [--repair]
"""

import argparse
import sys
import time
from typing import Iterable, List, Tuple

import psycopg
from backfill_funding_provenance import PAGE, POSTGRES_URI, btc_to_sat, core_transactions


def legacy_to_sat(value) -> int:
    """The old conversion, reproduced exactly so its output can be recognised."""
    return int(value * 100_000_000)


def pages(conn: psycopg.Connection) -> Iterable[List[Tuple[str, str, int, int]]]:
    """Closures with no recorded outputs, in block order.

    Block order matters: Core reads these from disk, and walking the chain forwards is
    far kinder to it than jumping around by gossip_id. The cursor is
    (closing_height, gossip_id) rather than "still has no outputs", because a closure
    this run refuses still has no outputs and would be re-selected forever.
    """
    height, gossip_id = -1, ""
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.gossip_id, c.closing_txid, c.closing_height, c.settled_balance_sat
                FROM channel_closures c
                WHERE (c.closing_height, c.gossip_id) > (%s, %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM channel_closure_outputs o WHERE o.gossip_id = c.gossip_id
                  )
                ORDER BY c.closing_height, c.gossip_id
                LIMIT %s
                """,
                (height, gossip_id, PAGE),
            )
            rows = cur.fetchall()
        if not rows:
            return
        height, gossip_id = rows[-1][2], rows[-1][0]
        yield rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill closing-transaction outputs")
    parser.add_argument("--apply", action="store_true", help="write; without it, resolve and report only")
    parser.add_argument(
        "--repair",
        action="store_true",
        help="also rewrite settled_balance_sat / output_0_sat / output_1_sat for closures "
        "whose stored sum matches only the old truncating arithmetic",
    )
    parser.add_argument("--limit", type=int, default=0, help="stop after this many closures (0 = all)")
    args = parser.parse_args()

    started = time.time()
    seen = written = outputs_written = repaired = 0
    missing_tx = refused = 0

    with psycopg.connect(POSTGRES_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*) FROM channel_closures c WHERE NOT EXISTS (
                       SELECT 1 FROM channel_closure_outputs o WHERE o.gossip_id = c.gossip_id)"""
            )
            total = cur.fetchone()[0]
        print(f"{total:,} closures need their outputs" + ("" if args.apply else "  (dry run)"))

        for page in pages(conn):
            transactions = core_transactions(sorted({txid for _, txid, _, _ in page}))

            rows: List[tuple] = []
            repairs: List[tuple] = []
            for gossip_id, closing_txid, _, settled in page:
                seen += 1
                tx = transactions.get(closing_txid)
                if tx is None:
                    missing_tx += 1
                    continue
                outputs = tx.get("vout") or []
                exact = sum(btc_to_sat(o["value"]) for o in outputs)
                legacy = sum(legacy_to_sat(o["value"]) for o in outputs)
                if settled is not None and settled not in (exact, legacy):
                    refused += 1
                    continue
                for vout in outputs:
                    script = vout.get("scriptPubKey") or {}
                    addresses = script.get("addresses") or ([script["address"]] if script.get("address") else [])
                    rows.append(
                        (
                            gossip_id,
                            closing_txid,
                            int(vout.get("n", 0)),
                            btc_to_sat(vout["value"]),
                            script.get("hex"),
                            script.get("type"),
                            addresses[0] if addresses else None,
                        )
                    )
                if settled is not None and settled != exact:
                    # Stored sum matches only the truncating arithmetic, so it is light by
                    # one satoshi per affected output. We are holding the real numbers.
                    significant = sorted(
                        (btc_to_sat(o["value"]) for o in outputs if btc_to_sat(o["value"]) > 546),
                        reverse=True,
                    )
                    repairs.append(
                        (
                            exact,
                            significant[0] if significant else 0,
                            significant[1] if len(significant) > 1 else 0,
                            gossip_id,
                        )
                    )
                written += 1

            repaired += len(repairs)
            if args.apply and rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO channel_closure_outputs
                           (gossip_id, closing_txid, vout_index, value_sat, script_pubkey, script_type, address)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (gossip_id, vout_index) DO NOTHING""",
                        rows,
                    )
                    if args.repair and repairs:
                        cur.executemany(
                            """UPDATE channel_closures
                               SET settled_balance_sat = %s, output_0_sat = %s, output_1_sat = %s
                               WHERE gossip_id = %s""",
                            repairs,
                        )
                conn.commit()
                outputs_written += len(rows)

            rate = seen / max(time.time() - started, 1e-9)
            print(
                f"  {seen:,}/{total:,}  ok={written:,}  {rate:.0f}/s  eta {(total - seen) / rate / 3600:.1f}h",
                end="\r",
                flush=True,
            )
            if args.limit and seen >= args.limit:
                break

    print(f"\n{seen:,} examined in {(time.time() - started) / 60:.1f} min")
    print(f"  {written:,} closing transactions verified against their stored balance")
    if args.apply:
        print(f"  {outputs_written:,} outputs recorded")
    else:
        print("  dry run; pass --apply to write")
    verb = "repaired" if (args.apply and args.repair) else "would be repaired with --repair"
    if repaired:
        print(f"  {repaired:,} settled_balance_sat {verb} (old truncating sum)")
    if missing_tx:
        print(f"  {missing_tx:,} closing transactions Core does not have")
    if refused:
        print(f"  {refused:,} output sums match neither arithmetic -- REFUSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
