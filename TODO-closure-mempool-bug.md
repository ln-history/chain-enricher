# chain-enricher: mempool spends corrupt 23,551 closure records

**Found 2026-08-25/26, verified against Bitcoin Core and Fulcrum. Not yet fixed.**

## The defect

`main.py`, `closure_worker()`, around line 392:

```python
history = fulcrum.call('blockchain.scripthash.get_history', [scripthash])
if not history or len(history) < 2:
    continue
history.sort(key=lambda x: x['height'])
closing_event = history[-1]          # <-- assumes the spend sorts last
```

In the Electrum protocol an **unconfirmed transaction is reported with `height` 0**
(or `-1` when it has unconfirmed parents). Those values sort *below* every confirmed
height, so when the closing transaction is still in the mempool the sort produces
`[spend(0), funding(963947)]` and `history[-1]` returns the **funding** transaction.

Everything downstream is then derived from the funding tx: `closing_txid`,
`closing_height`, `closing_timestamp`, the mutual/force classification, and all the
financial fields.

The code is aware mempool exists but only guards the wrong place — `if closing_height
<= 0: pass` sits *after* the selection has already gone wrong.

## Verification

For scid `1059870935167991809` (funding block 963947) Fulcrum today returns:

```
height 963947  8a45a360…  FUNDING
height 964034  d45aad22…  spend
```

Sorted by height, `history[-1]` is now correctly the spend. The database nevertheless
holds the funding txid — which is only possible if the spend carried height 0 at scan
time. Confirmed on three sampled channels; the spend confirmed 32–106 blocks after
funding in each case, i.e. these were scanned in the window between broadcast and
confirmation.

Independently confirmed from the chain: for every sampled affected row the tx at
`closing_txid` has, at the scid's output index, a `witness_v0_scripthash` output whose
value equals `channels.capacity_sat` **exactly** — the 2-of-2 funding output. The
channels are genuinely closed (funding outputs are spent); only the recorded transaction
is wrong.

## Blast radius

Detector (exact, cheap — a channel cannot close in its own funding block):

```sql
SELECT count(*) FROM channels c JOIN channel_closures cl ON cl.gossip_id = c.gossip_id
WHERE cl.closing_height = (c.scid >> 40);
```

**23,551 rows (4.6% of 508,258 channels).** Consequences:

| field | state |
|---|---|
| `closing_timestamp` | = `funding_timestamp` in 23,548 rows → **zero lifetime** |
| `closing_height`, `closing_txid` | the funding tx |
| `type` (mutual/force) | classified from the funding tx's input — meaningless. Currently 15,288 force / 8,263 mutual |
| `mining_fee_sat` | **0 in 23,550 of 23,551** (vs 38 of 441,809 for correct closures) |
| `settled_balance_sat` | avg 108.5M sat vs 13.1M for correct rows — 8× inflated, it is summing funding outputs |

**Zero lifetime is the worst of these.** `funding_timestamp <= T AND closing_timestamp > T`
can never be true, so these channels are invisible to every point-in-time query — the
snapshot endpoint, any graph build, any open-channel count silently omits them.

`mining_fee_sat = 0` is a second, independent signature of the same rows.

**This is ongoing.** The newest affected channel closed at height 963947 (2026-08-25).
Every channel whose close is observed while still in the mempool is corrupted.

## The fix

Two independent errors to correct.

**1. Mempool entries must sort last, not first.** They are the newest events.

**2. Do not select by ordering at all.** The funding height is known from the scid, so the
close is the *earliest confirmed* history entry above it. This is robust to splices and
to multi-entry histories, which the `len(history) < 2` heuristic is not.

```python
funding_height = scid >> 40

# Electrum reports unconfirmed txs as height 0 / -1. For an archive, ignore them
# entirely: a mempool spend can still be replaced, and the row will be revisited
# on the next pass once it confirms.
confirmed = [e for e in history if e['height'] > funding_height]
if not confirmed:
    continue                      # not yet closed, or close not yet confirmed
closing_event = min(confirmed, key=lambda e: e['height'])
```

This also removes the need for the `len(history) < 2` test and the `closing_height <= 0`
branch below it.

**3. Add a guard so this class of bug cannot be written again**, since the invariant is
free to check:

```python
if closing_event['height'] <= funding_height:
    logger.error("refusing closure at/below funding height for scid %s", scid)
    continue
```

Optionally add a counter (`chain_closure_rejected_total`) so violations are visible in
Grafana rather than silent.

## Backfill

Per affected channel: derive the scripthash with the existing
`get_p2wsh_scripthash(bitcoin_key_1, bitcoin_key_2)`, fetch history, select the earliest
confirmed entry above `scid >> 40`, fetch that tx verbose, recompute
`closing_txid / closing_height / closing_timestamp / type / settled_balance_sat /
mining_fee_sat / output_0_sat / output_1_sat`, then update `channel_closures` and
`channels.closing_timestamp` **in one transaction per batch**.

**Cost.** Measured Fulcrum round-trip over Tailscale: **82 ms/call, ~12 calls/s
sequential**. This is latency-bound, not throughput-bound.

| approach | calls | wall clock |
|---|---:|---|
| sequential | 47,102 | ~65 min |
| 8 parallel connections | 47,102 | ~8 min |
| JSON-RPC batching, 100/request | ~470 requests | **well under a minute** |

Fulcrum supports JSON-RPC batch requests and `main.py` already has a batching helper for
Core (`rpc_batch_request`). Batching is the right approach; parallel connections are the
fallback.

Database side: 23,551 updates across `channel_closures` (225 MB) and `channels` (1.7 GB) —
both small, a few minutes, no bloat concern at this scale.

**Back up first**, as with the 2026-08-25 node-announcement repair:

```sql
CREATE TABLE repair_YYYYMMDD_closures AS
  SELECT cl.* FROM channel_closures cl JOIN channels c ON c.gossip_id = cl.gossip_id
  WHERE cl.closing_height = (c.scid >> 40);
```

## Edge cases

- **Spend still unconfirmed.** Skip; the fixed worker picks it up next pass. Expect a
  small residue after the backfill — re-run the detector to size it.
- **Splices / multi-entry histories.** `min(height > funding_height)` takes the first
  post-funding event, which is the correct close for a simple channel. A spliced channel
  needs a product decision about what "closed" means; count them first with
  `len(history) > 2`.
- **Channels whose funding output is still unspent.** `gettxout` returns non-null;
  these should not be in `channel_closures` at all. Worth counting during the backfill.
- **`type` reclassification.** All 23,551 classifications are unreliable and must be
  recomputed, not preserved.

## Sequencing

1. Fix `closure_worker()`, `cz bump`, rebuild, push, recreate — **before** the backfill,
   so the worker does not keep writing bad rows while the repair runs.
2. Back up the affected rows.
3. Run the backfill batched, verify, then re-run the detector — it should return 0 plus
   any rows whose spends were unconfirmed at the time.
4. Add the `mining_fee_sat = 0` rate and the detector count to monitoring.

---

## RESOLVED 2026-08-26

Fixed in **0.6.1** and backfilled. Detector 23,551 → 6 (residual = spends still
unconfirmed). See the ln-history-database skill for the full before/after table.

**Correction to the plan above.** The backfill as scoped selected the close by *height*
(earliest confirmed entry above the funding height). That mis-selected 36 of 23,573 rows
(0.15%): an Electrum scripthash identifies a script, not an outpoint, so it can carry
activity unrelated to this channel's funding output. The scope document's own first
instinct — match the input spending `(funding_txid, scid & 0xFFFF)` — was correct and
should have been used from the start. All 36 were found via
`mining_fee_sat > capacity_sat * 0.05` and repaired by outpoint matching.

**The live worker still uses height matching.** It is correct for the overwhelming
majority and much cheaper (no per-candidate tx fetch), but it carries the same 0.15%
failure mode. Consider adding outpoint verification for the selected candidate only —
one extra `blockchain.transaction.get` per detected close, which the worker already does
anyway to compute financials. That would make it exact at no extra RPC cost.

**Still open:** `mining_fee = max(0, capacity_sat - sum(vout))` flags 31,293 of 442,062
(7.1%) untouched closure rows as implausible. Separate bug, not investigated.

## 0.6.2 — outpoint verification in the live worker (2026-08-26)

The worker now verifies the candidate spends `(funding_txid, scid & 0xFFFF)` before
accepting it, at no extra RPC cost. Two new `chain_closure_rejected_total` reasons:
`no_funding_tx_at_height`, `no_tx_spends_funding_outpoint`.

This mattered more than expected: before 0.6.2 the worker *re-closed* a channel whose
bad closure row had just been deleted (`scid 761140222877237249`, funding output still
unspent). Any repair of closure data must therefore ship the worker fix first.

**The "7.1% bad fees" item is withdrawn** — it was a ratio-threshold artifact, not a bug;
the fee formula is correct (200/200 verified on-chain). Real corruption is found with an
absolute threshold: `mining_fee_sat > 1_000_000` gave 33 rows, 14 genuinely corrupt, all
repaired. Fees ≥ 0.1 BTC: 9 → 0. `mining_fee_sat >= capacity_sat`: 1 → 0.
