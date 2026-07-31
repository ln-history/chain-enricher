# Handoff: populate `channels.announceable_timestamp` in `chain-enricher`

**Audience:** an agent working in `chain-enricher/`.
**Goal:** make this service continuously fill a new DB column, `channels.announceable_timestamp`,
for channels where it is `NULL` — so it stays populated for all future channels.
**Status when this was written (2026-07-31):** the column exists and was one-time back-filled for
all ~500k existing channels. Nothing writes it on an ongoing basis yet — that is your job.

---

## 1. What the column means

`announceable_timestamp` = the **block-header timestamp of the block at height `(scid >> 40) + 5`**.

- `scid >> 40` is the funding block height (standard LN SCID encoding; see `decode_scid` in `main.py`).
- `+ 5` → the block at which the funding tx reaches its **6th confirmation** (funding block = conf 1,
  so conf 6 is 5 blocks later). This is the BOLT 7 "SHOULD have 6 confirmations before announcing"
  threshold — i.e. the earliest instant a channel is *announceable*.
- It exists to measure **channel_announcement propagation lag**:
  `lag = first seen_at of the announcement − announceable_timestamp`.

DB column (already created, do **not** re-create):
```
channels.announceable_timestamp  timestamptz  NULL
```
It has a `COMMENT`; read it with `\d+ channels` or `col_description`.

---

## 2. Hard design rules (read before coding)

1. **Derive it from `scid` ONLY — never from `funding_timestamp`.**
   `funding_timestamp` is **corrupt for ~18 channels** (off by days to >1 year; some hold a
   sub-second wall-clock value instead of a block time). The one-time backfill was first attempted
   by reusing `funding_timestamp` as a block-time "self-map" and it got poisoned by exactly those
   rows. `announceable_timestamp = blockheader(scid_block + 5).time`, full stop.

2. **Use `getblockhash` → `getblockheader` (header-only). NEVER `getblock` or `getblockstats`.**
   You only need the header `time`. `getblockheader` reads the in-memory block index (~1–2k
   blocks/s). `getblockstats`/`getblock` load the full block body and are ~**100× slower**
   (measured: 4 blocks/s vs ~1000). The funding slow-path in `main.py` uses `getblock` because it
   needs the tx — you do **not**.

3. **It is independent of funding/closure enrichment.** `scid` is all you need — a channel can get
   `announceable_timestamp` even if `capacity_sat`/`funding_timestamp` are still NULL. Do **not**
   gate it on funding being enriched.

4. **`block+5` is essentially always already mined** for an announced channel (nodes announce after
   6 confs), so there is normally no wait. But handle "block not found yet" gracefully (skip →
   retried next loop) for spec-violating early announcements or a briefly-lagging node. Never crash
   on a missing block.

5. **Idempotent + batched.** Select a batch of `announceable_timestamp IS NULL` channels, resolve
   distinct target heights once, batch the RPC, `UPDATE ... WHERE ... announceable_timestamp IS NULL`.

---

## 3. Recommended implementation

Add a third enrichment step to the existing `funding_worker` loop (least code, reuses the Bitcoin
Core config + `rpc_batch_request`). A separate `announceable_worker()` thread is also fine if you
prefer isolation — same body, own `psycopg.connect(..., autocommit=True)`.

### 3a. Helper (paste near the other funding helpers)

```python
ANNOUNCEABLE_CONF_OFFSET = 5  # block (funding_height + 5) = 6th confirmation (BOLT 7 announceable)


def _enrich_announceable(conn: psycopg.Connection) -> int:
    """Populate channels.announceable_timestamp = header time of block ((scid>>40)+5).

    Purely scid-derived (NOT from funding_timestamp, which is corrupt for a few channels).
    Bitcoin Core getblockhash -> getblockheader (header-only; never getblock/getblockstats).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT gossip_id, scid FROM channels "
            "WHERE scid IS NOT NULL AND announceable_timestamp IS NULL "
            f"LIMIT {BATCH_SIZE}"
        )
        rows = cur.fetchall()
    if not rows:
        return 0

    heights: dict[int, list[str]] = {}  # target height -> gossip_ids needing it
    for gid, scid_int in rows:
        h = (scid_int >> 40) + ANNOUNCEABLE_CONF_OFFSET
        heights.setdefault(h, []).append(gid)
    ordered = sorted(heights)

    # height -> block hash (id-mapped: JSON-RPC batch order is not guaranteed)
    hash_resp = rpc_batch_request("getblockhash", ordered)
    if not hash_resp:
        return 0
    hash_by_id = {r["id"]: r.get("result") for r in hash_resp if isinstance(r, dict)}
    hh: list[int] = []
    header_reqs: list[list[str]] = []
    for i, h in enumerate(ordered):
        block_hash = hash_by_id.get(i)
        if block_hash:  # None => block not mined yet; leave NULL, retried next loop
            hh.append(h)
            header_reqs.append([block_hash])
    if not header_reqs:
        return 0

    # block hash -> header (contains "time")
    hdr_resp = rpc_batch_request("getblockheader", header_reqs)
    if not hdr_resp:
        return 0
    hdr_by_id = {r["id"]: r.get("result") for r in hdr_resp if isinstance(r, dict)}

    updates = 0
    with conn.cursor() as cur:
        for i, h in enumerate(hh):
            res = hdr_by_id.get(i)
            if not res or "time" not in res:
                continue
            ts = datetime.fromtimestamp(res["time"], timezone.utc)
            for gid in heights[h]:
                cur.execute(
                    "UPDATE channels SET announceable_timestamp = %s "
                    "WHERE gossip_id = %s AND announceable_timestamp IS NULL",
                    (ts, gid),
                )
                updates += cur.rowcount
    return updates
```

### 3b. Call it in `funding_worker`'s loop

After the slow (Bitcoin Core) path block, before `if not did_work:`:

```python
            # === ANNOUNCEABLE: timestamp of block (funding_height + 5), scid-derived ===
            ann = _enrich_announceable(conn)
            if ann:
                logger.info(f"Set announceable_timestamp on {ann} channels")
                ANNOUNCEABLE_ENRICHED.inc(ann)
                did_work = True
```

### 3c. Metric (next to the other funding metrics)

```python
ANNOUNCEABLE_ENRICHED = Counter(
    'chain_announceable_enriched_total',
    'Total channels given an announceable_timestamp (block funding+5)'
)
```

---

## 4. Operational: add a partial index (needs the admin/migration role)

After the initial backfill, only *new* channels are NULL — but the polling query still scans
`channels` for `announceable_timestamp IS NULL` every loop. Make it cheap with a partial index that
holds only the pending rows:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_channels_announceable_pending
    ON channels (scid)
    WHERE announceable_timestamp IS NULL AND scid IS NOT NULL;
```

(`CONCURRENTLY` so it doesn't block ingest. This is a schema change → use the admin role, not
`ai_reader`. See the **ln-history-database** skill for the migration-role connection recipe.)

---

## 5. Verification

1. **Build/lint:** `black main.py && ruff check --fix main.py && mypy main.py` (line length 120).
2. **Run locally** against a dev DB + Bitcoin Core (`python main.py`), watch for
   `Set announceable_timestamp on N channels`.
3. **Drift trends to zero:**
   ```sql
   SELECT count(*) FROM channels WHERE scid IS NOT NULL AND announceable_timestamp IS NULL;
   ```
4. **Sanity of values** — the gap must be a small positive ~5-block interval (median ≈ 46 min):
   ```sql
   SELECT
     round(EXTRACT(EPOCH FROM percentile_cont(0.5)
       WITHIN GROUP (ORDER BY announceable_timestamp - funding_timestamp))/60,1) AS median_gap_min
   FROM channels WHERE announceable_timestamp IS NOT NULL;
   ```
   Strongly-negative or `>1 day` gaps are **not** your bug — they flag the corrupt-`funding_timestamp`
   channels (announceable is correct; funding is the bad field). This column is, usefully, a detector
   for those.

---

## 6. Don't forget

- **Env:** no new variables — reuse `BITCOIN_RPCUSER/PASSWORD/HOST/PORT` (already wired into
  `BTC_RPC_URL` / `rpc_batch_request`).
- **Docs:** update `CLAUDE.md` "What This Service Does" to mention the third enrichment
  (announceable timestamp), and note the new metric.
- **Commit:** Conventional Commits, then `cz bump` (pep621 version in `pyproject.toml`).
- **Out of scope (mention to the owner, don't fix here):** the ~18 channels with a corrupt
  `funding_timestamp`. Repairing `funding_timestamp` itself is a separate task; this handoff only
  adds `announceable_timestamp`.

---

## 7. Alternative (if you prefer Fulcrum over Bitcoin Core)

The closure worker already reads block header timestamps from Fulcrum:
`fulcrum.call('blockchain.block.header', [height])` returns the raw 80-byte header hex; the timestamp
is a 4-byte little-endian int at byte offset 68:
```python
ts_int = int.from_bytes(bytes.fromhex(header)[68:72], 'little')
```
This works and avoids Bitcoin Core, but it is one round-trip per height (no batching in the raw
socket client) and needs `height <= tip`. Bitcoin Core `getblockhash`+`getblockheader` (batched) is
recommended; use Fulcrum only if you're consolidating on it.
