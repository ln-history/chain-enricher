-- Record where a channel's money came from, and where it went.
--
-- MOTIVATION
--   To classify a node as a liquidity source or sink you need two facts per channel:
--   who funded it (the funder starts with 100% of the capacity) and who held what when
--   it closed. Neither is in BOLT 7 gossip -- channel_announcement orders node_id_1 and
--   node_id_2 lexicographically and deliberately says nothing about who opened the
--   channel -- so both have to come from the chain.
--
--   This adds the raw material. It does not attribute anything by itself; attribution is
--   built on top, by joining a channel's funding inputs to earlier channels' closing
--   outputs (operators recycle capital, so those chains are dense).
--
-- WHAT WAS WRONG WITH WHAT WE HAD
--   channel_closures.balance_node_1_sat / balance_node_2_sat claim a per-node split and
--   do not have one. chain-enricher wrote them from `sorted(outputs, reverse=True)[0:2]`
--   -- the same two values it wrote into output_0_sat / output_1_sat. Verified across all
--   468,123 rows: balance_node_1_sat = output_0_sat and balance_node_1_sat >=
--   balance_node_2_sat, without exception, and zero channels close "all to node_2", which
--   is impossible if the labels were real.
--
--   The sort also destroys the real vout order. Closing tx b3f91c40...7bc5 has
--   vout[0] = 2,000,632 and vout[1] = 197,998,100 on chain; the table stores
--   output_0_sat = 197,998,100.
--
--   The existing columns are left alone here -- the API reads them, so redefining them
--   silently would be worse than the current state. They are documented as what they
--   actually are, and the honest data goes in the new table beside them.

BEGIN;

-- ---------------------------------------------------------------- funding provenance

ALTER TABLE channels ADD COLUMN IF NOT EXISTS funding_txid varchar(64);

COMMENT ON COLUMN channels.funding_txid IS
'Transaction that created this channel''s funding output. Derivable from the scid
(block_height << 40 | tx_index << 16 | output_index) but stored so the funding inputs can
be joined without a Bitcoin Core round trip. NULL until chain-enricher or the backfill
fills it.';

CREATE INDEX IF NOT EXISTS channels_funding_txid ON channels (funding_txid);

CREATE TABLE IF NOT EXISTS channel_funding_inputs (
    gossip_id  varchar(64) NOT NULL,
    vin_index  integer     NOT NULL,
    prev_txid  varchar(64) NOT NULL,
    prev_vout  integer     NOT NULL,
    PRIMARY KEY (gossip_id, vin_index)
);

COMMENT ON TABLE channel_funding_inputs IS
'The outpoints a channel''s funding transaction spent -- the evidence of who funded it.
One row per input, in wire order. The funder is whoever controlled these coins, which is
resolved by joining prev_txid/prev_vout against channel_closure_outputs: an operator
closing one channel to open another leaves exactly that link. No foreign key to channels,
deliberately -- a funding tx can be recorded before its channel_announcement is seen.';

-- The attribution join runs in this direction: given a closing output, which channel did
-- it fund next?
CREATE INDEX IF NOT EXISTS channel_funding_inputs_prev
    ON channel_funding_inputs (prev_txid, prev_vout);

-- ----------------------------------------------------------------- closing provenance

CREATE TABLE IF NOT EXISTS channel_closure_outputs (
    gossip_id      varchar(64) NOT NULL,
    closing_txid   varchar(64) NOT NULL,
    vout_index     integer     NOT NULL,
    value_sat      bigint      NOT NULL,
    script_pubkey  text,
    script_type    text,
    address        text,
    PRIMARY KEY (gossip_id, vout_index)
);

COMMENT ON TABLE channel_closure_outputs IS
'Every output of a channel''s closing transaction, in real vout order, with its script.
This is what channel_closures.output_0_sat / output_1_sat should have been: those two are
the largest and second-largest output by value, not vout 0 and vout 1.

The script is the point. Attributing a closing balance to a node means matching an output
to something that node controls, and that is impossible after the fact without the
scriptPubKey -- recovering it later would mean re-fetching all 468k closing transactions.

closing_txid is denormalised from channel_closures so the funding-input join needs only
this table.';

CREATE INDEX IF NOT EXISTS channel_closure_outputs_txid_vout
    ON channel_closure_outputs (closing_txid, vout_index);
CREATE INDEX IF NOT EXISTS channel_closure_outputs_address
    ON channel_closure_outputs (address);

-- --------------------------------------------------------- correct the existing lies

COMMENT ON COLUMN channel_closures.output_0_sat IS
'LARGEST output of the closing transaction by value, NOT vout[0]. chain-enricher sorts
outputs descending and drops dust below 546 sat (anchors). For real vout order and the
scripts, use channel_closure_outputs.';

COMMENT ON COLUMN channel_closures.output_1_sat IS
'SECOND-LARGEST output by value, NOT vout[1]. See output_0_sat.';

COMMENT ON COLUMN channel_closures.balance_node_1_sat IS
'MISLEADING NAME -- this is not attributed to node_1. It is written from the same value as
output_0_sat, i.e. the largest output. Verified across all 468,123 rows:
balance_node_1_sat = output_0_sat and >= balance_node_2_sat without exception, and no
channel closes "all to node_2", which could not happen if the label were real. Retained
because the API selects it. Do not use it to reason about which node held what.';

COMMENT ON COLUMN channel_closures.balance_node_2_sat IS
'MISLEADING NAME -- the smaller output, not node_2''s balance. See balance_node_1_sat.
It is a valid LOWER BOUND on how much of the capacity moved away from the funder, since
the funder began with all of it; that much is sound and is all it supports.';

GRANT SELECT ON channel_funding_inputs, channel_closure_outputs
    TO ai_reader, grafanareader, yannik;

COMMIT;
