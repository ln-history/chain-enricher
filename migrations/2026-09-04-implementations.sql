-- Which implementation is each node running?
--
-- MOTIVATION
--   A node_announcement carries a BOLT 9 feature bitfield, and implementations differ in
--   which bits they advertise and whether they mark them required or optional. That
--   pattern is a fingerprint. Alex Myers' impscan
--   (https://github.com/endothermicdev/impscan, BSD-3-Clause, commit d2cf119) encodes the
--   heuristics; its README asks for exactly what this adds: "storage to document changes
--   in network implementation usage over time".
--
-- WHY A LOOKUP TABLE AND NOT A COLUMN
--   The fingerprint is a pure function of the bitfield, and the bitfield is far from
--   unique: 32,511,476 node announcements in this archive carry just **508 distinct
--   bitfields**. Storing the answer per announcement would repeat each of 508 results
--   ~64,000 times, and re-running the heuristics -- which impscan's README says will be
--   necessary, "routinely" -- would rewrite a 45 GB table instead of 508 rows.
--
--   So the fingerprint lives keyed by the bitfield, and node_announcements is not touched
--   at all. Nothing needs backfilling: history acquires fingerprints the moment the
--   lookup table is populated, because it is a join.
--
-- WHY THE KEY IS A HASH
--   Bitfields reach 6,622 bytes in this archive. A btree index entry is capped at 2,704
--   bytes, and the only reason a plain `features bytea PRIMARY KEY` works today is that
--   these values are mostly zero and compress under the cap -- which an incompressible
--   3 KB bitfield would not. Keying on sha256 is 32 bytes regardless.
--
-- WHAT THIS DOES NOT CLAIM
--   Feature bits are self-reported and unauthenticated. A node may advertise anything,
--   and several heuristics are *negative* rules matching on the absence of a feature,
--   which name no implementation at all. Those are recorded as 'Unknown' rather than
--   dressed up as a result.

BEGIN;

-- ------------------------------------------------------------------ the rules, versioned

CREATE TABLE IF NOT EXISTS feature_rulesets (
    ruleset     text        PRIMARY KEY,
    source      text        NOT NULL,
    strict      boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    notes       text
);

COMMENT ON TABLE feature_rulesets IS
'One row per revision of the fingerprinting rules. impscan''s README warns the heuristics
"are apt to break and should be routinely updated", so a stored fingerprint is meaningless
without knowing which revision produced it. Re-fingerprinting under new rules inserts a new
ruleset and a new set of feature_fingerprints rows; the old ones stay, so a change in the
measured implementation share can be told apart from a change in the network.';

COMMENT ON COLUMN feature_rulesets.strict IS
'Whether Feature.SET was enforced. impscan declares that requirement but never tests it --
its Heuristic.test has no branch for it -- so four of twelve heuristics (CLN, CLN v24.02+,
CLN v25.05+, Eclair) are more permissive than they read. Verified 2026-09-04: enforcing it
changes the fingerprint of 0 of 508 bitfields in this archive, because the features it
guards are ones essentially every node advertises. Recorded because it is inert here, not
because it is inert in principle.';

-- ------------------------------------------------------------- bitfield -> implementation

CREATE TABLE IF NOT EXISTS feature_fingerprints (
    features_sha256 bytea NOT NULL,
    ruleset         text  NOT NULL REFERENCES feature_rulesets(ruleset) ON DELETE CASCADE,
    features        bytea NOT NULL,
    heuristic       text,
    implementation  text  NOT NULL,
    all_matches     text[],
    first_seen      timestamptz,
    PRIMARY KEY (features_sha256, ruleset)
);

COMMENT ON TABLE feature_fingerprints IS
'One row per (distinct feature bitfield, ruleset). 508 bitfields cover the whole archive.
Join to node_announcements on the bitfield -- ON f.features = na.features -- which is a hash
join and needs no index on the 45 GB side.';

COMMENT ON COLUMN feature_fingerprints.heuristic IS
'Name of the FIRST impscan heuristic the bitfield matched, or NULL for "indef". First,
because the heuristics are not mutually exclusive and impscan resolves that by order: the
README''s own example matches both "CLN" and "2200" and is reported as CLN only because CLN
is listed earlier.';

COMMENT ON COLUMN feature_fingerprints.implementation IS
'Implementation family: LND, CLN, LDK, Eclair, Electrum, nlightning, or Unknown. Unknown is
a real answer, not a gap -- two heuristics ("No OPTION_SHUTDOWN_ANYSEGWIT" and "2200") match
on the ABSENCE of a feature and so identify nobody. On current announcement heads they cover
26.8% of nodes, which would badly overstate any implementation they were folded into.';

COMMENT ON COLUMN feature_fingerprints.all_matches IS
'Every heuristic the bitfield matched, in order. This is how a confident fingerprint is told
from a coincidence: one match is evidence, three overlapping matches are an ordering
artefact.';

CREATE INDEX IF NOT EXISTS feature_fingerprints_implementation
    ON feature_fingerprints (ruleset, implementation);

-- --------------------------------------------- what a bit proves about the running version

CREATE TABLE IF NOT EXISTS feature_bit_origins (
    implementation text    NOT NULL,
    bit            integer NOT NULL,
    first_version  text    NOT NULL,
    first_released date,
    source         text    NOT NULL,
    verified_on    date    NOT NULL,
    notes          text,
    PRIMARY KEY (implementation, bit)
);

-- Added after the table shipped; harmless to re-run.
ALTER TABLE feature_bit_origins ADD COLUMN IF NOT EXISTS first_released date;

COMMENT ON COLUMN feature_bit_origins.first_released IS
'Commit date of first_version. The version floor must rank by THIS, not by bit number: a
later release can add a lower bit (lnd took route blinding, bit 24, in v0.18.0-beta having
taken keysend, bit 54, in v0.15.0-beta), so ranking by bit would understate the floor.';

COMMENT ON TABLE feature_bit_origins IS
'The earliest release of an implementation that could advertise a given feature bit, read
from that implementation''s source at its release tags.

The direction matters. A version does NOT map to a bitfield: what a node advertises depends
on build flags and runtime configuration (lnd''s --protocol.* options, CLN''s
EXPERIMENTAL_FEATURES), so the same release produces many different bitfields. The inverse
IS sound and monotone: a node advertising a bit first implemented in release R is running R
or later, and the maximum over its advertised bits is a lower bound on its version.

The same table supports elimination: a bit an implementation never advertises in any release
is evidence the node is not running it.

Populated by scripts/extract_feature_bit_origins.py, which reads each implementation''s
source at every release tag. Every one of them marks the node_announcement context
explicitly, which is what makes this tractable: lnd''s SetNodeAnn, CLN''s
NODE_ANNOUNCE_FEATURE, Eclair''s NodeFeature trait, LDK''s NodeContext.

Tags are ordered by COMMIT DATE, not by parsing the version string. CLN carries a v0.0.1
tag whose commit is from 2022-08-25; ordering by name put it first and dated every CLN bit
to it, which is how OPT_ZEROCONF briefly came out as "introduced in v0.0.1".

BITS DO NOT MEAN THE SAME THING TO EVERY IMPLEMENTATION. Bit 30 is option_amp to lnd and
taproot to LDK; bit 50 is zeroconf everywhere except Eclair, where it is trampoline_payment;
bit 40 is peer-backup-storage to CLN and zero-fee-commitments to Eclair. So the primary key
is (implementation, bit) and a floor is only meaningful once the implementation is fixed.

Also seeded with a signal that needs no source archaeology -- see the inbound-fee row.';

-- lnd carries inbound fees in TLV record 55555 of channel_update, added in lnd 0.18.
-- Verified against this archive on 2026-09-04 by cross-tabulating against the independent
-- feature-bit fingerprint: of the nodes emitting the record, 99.4% fingerprint as LND. The
-- converse does not hold -- only 7.9% of LND nodes emit it -- so this is a high-precision,
-- low-recall test. It is recorded as a bit origin because it behaves like one: presence
-- proves a version floor, absence proves nothing.
INSERT INTO feature_bit_origins (implementation, bit, first_version, source, verified_on, notes)
VALUES ('LND', 55555, '0.18',
        'lnd TLV record 55555 in channel_update extra opaque data',
        DATE '2026-09-04',
        'Not a BOLT 9 node_announcement feature bit but a channel_update TLV record, keyed '
        'here by its record number because it carries the same kind of evidence. Measured: '
        '99.4% of emitters fingerprint as LND, 7.9% of LND nodes emit it.')
ON CONFLICT (implementation, bit) DO NOTHING;

-- ------------------------------------------------------------------------- reading it back

CREATE OR REPLACE VIEW node_implementations AS
SELECT na.node_id,
       na.valid_from,
       na.valid_to,
       f.ruleset,
       f.heuristic,
       f.implementation,
       na.features
FROM node_announcements na
JOIN feature_fingerprints f ON f.features = na.features;

COMMENT ON VIEW node_implementations IS
'Implementation per node announcement, over the SCD history: valid_to IS NULL is the current
head. Reads node_announcements (the filtered table the API serves). Point it at
node_announcements_complete instead if you need every announcement rather than the current
view of each node.';

GRANT SELECT ON feature_rulesets, feature_fingerprints, feature_bit_origins, node_implementations
    TO ai_reader, grafanareader, yannik;

COMMIT;
BEGIN;

CREATE OR REPLACE VIEW node_version_floor AS
WITH live AS (
    SELECT ni.node_id, ni.implementation, na.features, na.valid_from, na.valid_to
    FROM node_implementations ni
    JOIN node_announcements na
      ON na.node_id = ni.node_id AND na.valid_from = ni.valid_from
),
hit AS (
    SELECT l.node_id, l.implementation, l.valid_from, l.valid_to,
           o.first_version, o.first_released, o.bit
    FROM live l
    JOIN feature_bit_origins o ON o.implementation = l.implementation
    -- BOLT 9 numbers bit 0 as the least significant bit of the LAST byte, while
    -- get_bit() numbers bytes left to right and bits LSB-first within each byte.
    WHERE o.bit < length(l.features) * 8
      AND o.first_released IS NOT NULL
      AND get_bit(l.features, (length(l.features) - 1 - o.bit / 8) * 8 + (o.bit % 8)) = 1
)
SELECT DISTINCT ON (node_id, valid_from)
       node_id, valid_from, valid_to, implementation,
       first_version  AS version_at_least,
       first_released AS released_on,
       bit            AS deciding_bit
FROM hit
ORDER BY node_id, valid_from, first_released DESC, bit DESC;

COMMENT ON VIEW node_version_floor IS
'The earliest release of its own implementation that a node could be running, from the
feature bits it advertises. A LOWER BOUND and never an equality: a bit exists in the source
from some release onwards and never earlier, so advertising it proves the node is at least
that new -- but a node can run a much later release and advertise nothing that says so,
because what gets advertised depends on build flags and configuration.

deciding_bit is the LATEST-RELEASED advertised bit, which is the one that sets the floor --
not the highest-numbered one. A later release can add a lower bit: lnd took route blinding
(bit 24) in v0.18.0-beta having taken keysend (bit 54) in v0.15.0-beta, so ranking by bit
number understates the floor.
Reading it alongside the version is worth the column: bits do NOT mean the same thing to
every implementation -- bit 30 is option_amp to lnd and taproot to LDK, and bit 50 is
zeroconf everywhere except Eclair, where it is trampoline_payment -- so a floor is only
meaningful once the implementation is fixed, which is why this view joins through
node_implementations rather than standing alone.

This is the only version granularity available for lnd, which is ~85% of the network:
impscan resolves CLN to v23.02/v24.02/v25.05 but has a single LND bucket.';

GRANT SELECT ON node_version_floor TO ai_reader, grafanareader, yannik;

COMMIT;
