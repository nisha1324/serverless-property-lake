-- Registration lag: how long ago were the sales in the latest release completed?
-- A sale reaches the register only after HM Land Registry processes the
-- application, so each release mixes recent and much older transfers.
-- Lag = whole months between the transfer month and the release month
-- (1 = the month before the release). Added (A) rows only.
WITH latest AS (
    SELECT max(release) AS release FROM changes
),
added AS (
    SELECT
        CASE WHEN c.new_build THEN 'new build' ELSE 'existing' END AS segment,
        date_diff('month',
                  strptime(c.transfer_month || '-01', '%Y-%m-%d'),
                  strptime(l.release || '-01', '%Y-%m-%d')) AS lag_months
    FROM changes AS c, latest AS l
    WHERE c.release = l.release
      AND c.record_status = 'A'
)
SELECT
    segment,
    CASE
        WHEN lag_months <= 1  THEN '1 month'
        WHEN lag_months = 2   THEN '2 months'
        WHEN lag_months = 3   THEN '3 months'
        WHEN lag_months <= 6  THEN '4-6 months'
        WHEN lag_months <= 12 THEN '7-12 months'
        ELSE '13+ months'
    END                                                         AS lag_bucket,
    min(lag_months)                                             AS sort_key,
    count(*)                                                    AS sales,
    round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY segment), 1) AS pct_of_segment
FROM added
GROUP BY segment, lag_bucket
ORDER BY segment, sort_key;
