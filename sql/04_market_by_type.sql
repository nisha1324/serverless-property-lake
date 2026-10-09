-- Prices and volumes by property type, from the current view.
-- Window: transfers in the 12 months before the latest release month.
-- Category A = standard sales at full market value. Category B = additional
-- price paid entries (repossessions, buy-to-let mortgages, sales to companies,
-- transfers under a power of sale). B isn't a standard full-market-value
-- sale, so it's excluded from market prices here, but its share is shown.
WITH latest AS (
    SELECT strptime(max(release) || '-01', '%Y-%m-%d') AS release_start FROM changes
),
recent AS (
    SELECT p.*
    FROM price_paid_current AS p, latest AS l
    WHERE p.date_of_transfer >= l.release_start - INTERVAL 12 MONTH
      AND p.date_of_transfer <  l.release_start
)
SELECT
    CASE property_type
        WHEN 'D' THEN 'Detached' WHEN 'S' THEN 'Semi-detached'
        WHEN 'T' THEN 'Terraced' WHEN 'F' THEN 'Flat/maisonette'
        ELSE 'Other' END                                                      AS property_type,
    count(*) FILTER (WHERE ppd_category = 'A')                                AS standard_sales,
    round(median(price) FILTER (WHERE ppd_category = 'A'))                    AS median_price,
    round(100.0 * avg((tenure = 'L')::INT) FILTER (WHERE ppd_category = 'A'), 1) AS pct_leasehold,
    round(100.0 * avg(new_build::INT) FILTER (WHERE ppd_category = 'A'), 1)   AS pct_new_build,
    count(*) FILTER (WHERE ppd_category = 'B')                                AS additional_entries,
    round(100.0 * avg((ppd_category = 'B')::INT), 1)                          AS pct_additional
FROM recent
GROUP BY ALL
ORDER BY standard_sales DESC;
