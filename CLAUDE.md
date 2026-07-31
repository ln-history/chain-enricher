# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Service Does

`chain-enricher` is a long-running Python service that enriches a Lightning Network channel database with on-chain data. It runs two concurrent daemon threads:

1. **Funding Worker** — finds channels in PostgreSQL with `capacity_sat IS NULL`, decodes their SCID (Short Channel ID) into `(block, tx, output)` coordinates, batch-queries Bitcoin Core RPC to retrieve the funding transaction, then writes `capacity_sat` and `funding_timestamp` back. In the same loop it also backfills `announceable_timestamp` for any channel where it is `NULL` — the block-header time of block `(funding_height + 5)`, i.e. the funding tx's 6th confirmation (BOLT 7's "announceable" threshold) — derived purely from the SCID via `getblockhash`→`getblockheader` (see `_enrich_announceable`).
2. **Closure Worker** — scans open channels (those with `closing_timestamp IS NULL`) by deriving their P2WSH 2-of-2 multisig scripthash from `bitcoin_key_1`/`bitcoin_key_2`, queries a Fulcrum Electrum server for spend history, fetches the closing transaction, classifies it as `mutual` or `force` via the input sequence number (RFC BOLT 3), and writes results to `channel_closures`.

Prometheus metrics are exposed on port 8001.

## Running Locally

```bash
# Set up virtualenv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy and fill in .env
cp .env .env.local
# Then set POSTGRES_URI, BITCOIN_RPCUSER, BITCOIN_RPCPASSWORD, BITCOIN_RPCHOST, BITCOIN_RPCPORT, FULCRUM_HOST, FULCRUM_PORT

source .env.local
python main.py
```

## Docker

```bash
docker build -t chain-enricher .
docker run --env-file .env chain-enricher
```

## Linting & Formatting

```bash
# Format
black main.py

# Lint + auto-fix
ruff check --fix main.py

# Type check
mypy main.py
```

Line length is 120. Ruff rules: E, F, B (bugbear), I (isort). `E501` (line length) is ignored since Black handles it.

## Versioning & Commits

Commits follow [Conventional Commits](https://www.conventionalcommits.org/). Version is managed by [Commitizen](https://commitizen-tools.github.io/commitizen/) using `pep621` provider (version lives in `pyproject.toml`).

```bash
# Bump version
cz bump
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_URI` | — | PostgreSQL connection string |
| `BITCOIN_RPCUSER` | — | Bitcoin Core RPC user |
| `BITCOIN_RPCPASSWORD` | — | Bitcoin Core RPC password |
| `BITCOIN_RPCHOST` | — | Bitcoin Core host (`host.docker.internal` in Docker) |
| `BITCOIN_RPCPORT` | — | Bitcoin Core port (typically `8332`) |
| `FULCRUM_HOST` | `127.0.0.1` | Fulcrum Electrum server host |
| `FULCRUM_PORT` | `50001` | Fulcrum Electrum server port |
| `LOG_LEVEL` | `INFO` | Python logging level |

## Key Implementation Notes

- **SCID decoding** (`decode_scid`): SCID is a BigInt encoding `block << 40 | tx << 16 | output`. This is the standard Lightning Network SCID format.
- **P2WSH derivation** (`get_p2wsh_scripthash`): Keys are BIP67-sorted before building the 2-of-2 witness script. The Electrum scripthash is `sha256(scriptPubKey)` reversed.
- **Closure type heuristic**: Input sequence `>= 0xFFFFFFFE` → mutual close; anything lower encodes a commitment number → force close. Breach transactions look identical to force closes at this level.
- **`announceable_timestamp`** (`_enrich_announceable`): block-header timestamp of block `(scid >> 40) + 5` — the funding tx's 6th confirmation (BOLT 7's "SHOULD have 6 confirmations before announcing" threshold). Derived from the SCID **only**, never from `funding_timestamp` (corrupt for ~18 channels). Uses header-only `getblockheader` (never `getblock`/`getblockstats`, ~100× slower) and is independent of funding enrichment. Exists to measure channel_announcement propagation lag (`first seen_at − announceable_timestamp`). Metric: `chain_announceable_enriched_total`. A partial index `idx_channels_announceable_pending ON channels (scid) WHERE announceable_timestamp IS NULL AND scid IS NOT NULL` keeps the polling query cheap (create with the admin/migration role).
- **FulcrumClient**: A minimal raw TCP socket client. Responses are newline-delimited JSON-RPC.
- **`BATCH_SIZE = 50`** (funding worker); closure worker processes 20 random open channels per iteration.
- Both workers use `autocommit=True` PostgreSQL connections and hold them for the lifetime of the thread.
