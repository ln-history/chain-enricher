#!/usr/bin/env bash
# Wait for the funding backfill to finish, then run the closure-output backfill.
# Sequential on purpose: both are limited by the same remote Bitcoin Core disk, so
# running them together just splits one node's throughput two ways.
set -uo pipefail
cd /home/bitcoin/ln-history-research
M=/home/bitcoin/ln-history-research/chain-enricher/migrations
URI="postgresql://admin:zDEaKsvyshZd3TSPWbmt774duMhqQuHXQpgvcWN9@ln-history-database:5432/lnhistory"

# the range-partitioned workers all match this prefix; wait for every one of them
while docker ps --filter name=funding-backfill --format '{{.Names}}' | grep -q .; do sleep 60; done
echo "$(date -Is) funding backfill finished; starting closure outputs" >> "$M/backfill.log"

docker run --rm --name closure-backfill --network ln-history-network --env-file .env \
  -e POSTGRES_URI="$URI" -e FULCRUM_HOST=100.119.155.14 \
  -v "$M":/m ghcr.io/ln-history/chain-enricher:0.7.0 \
  python -u /m/backfill_closure_outputs.py --apply --repair > "$M/backfill_closures.log" 2>&1
echo "$(date -Is) closure backfill exited $?" >> "$M/backfill.log"
