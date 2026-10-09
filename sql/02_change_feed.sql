-- How each release's changes apply to the lake.
-- A C or D row should point at a transaction the lake already holds from an
-- earlier release. If it doesn't, the lake is missing history: the correction
-- or deletion is for a sale published before the lake started.
WITH earlier AS (
    SELECT DISTINCT c.release, c.transaction_id
    FROM changes AS c
    JOIN changes AS p
      ON p.transaction_id = c.transaction_id
     AND p.release < c.release
)
SELECT
    c.release,
    c.record_status,
    count(*)                                   AS rows,
    count(e.transaction_id)                    AS matches_earlier_release,
    count(*) - count(e.transaction_id)         AS no_earlier_record,
    round(median(c.price))                     AS median_price
FROM changes AS c
LEFT JOIN earlier AS e USING (release, transaction_id)
GROUP BY ALL
ORDER BY c.release, c.record_status;
