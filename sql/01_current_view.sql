-- Apply the A/C/D change feed.
-- Each monthly release lists changes, not a full snapshot: A = added,
-- C = changed (a corrected version of an earlier record), D = deleted.
-- The current state of the register is the latest version of each
-- transaction across all releases, with deletions removed.
-- `changes` is every curated row in the lake (all releases), registered by
-- lake/query.py over the Parquet files in S3.
CREATE OR REPLACE VIEW price_paid_current AS
SELECT * EXCLUDE (version_rank)
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY transaction_id ORDER BY release DESC) AS version_rank
    FROM changes
)
WHERE version_rank = 1
  AND record_status <> 'D';
