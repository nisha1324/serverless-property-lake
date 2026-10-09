-- The 15 counties with the most standard (category A) sales, with the median
-- price indexed to England and Wales overall (= 100).
-- Same 12-month window and current view as 04.
WITH latest AS (
    SELECT strptime(max(release) || '-01', '%Y-%m-%d') AS release_start FROM changes
),
recent AS (
    SELECT p.*
    FROM price_paid_current AS p, latest AS l
    WHERE p.date_of_transfer >= l.release_start - INTERVAL 12 MONTH
      AND p.date_of_transfer <  l.release_start
      AND p.ppd_category = 'A'
),
overall AS (
    SELECT median(price) AS median_all FROM recent
)
SELECT
    county,
    count(*)                                         AS sales,
    round(100.0 * count(*) / (SELECT count(*) FROM recent), 1) AS pct_of_sales,
    round(median(price))                             AS median_price,
    round(100 * median(price) / any_value(o.median_all)) AS price_index,
    round(any_value(o.median_all))                   AS england_wales_median
FROM recent, overall AS o
GROUP BY county
ORDER BY sales DESC
LIMIT 15;
