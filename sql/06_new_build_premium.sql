-- New-build premium, compared like for like.
-- A raw new-build vs existing median mixes different places and property
-- types (e.g. new flats are often in pricier city centres). Instead, compare
-- medians inside the same postcode district AND property type, keep only
-- cells with at least 5 sales on each side, and summarise the spread of the
-- per-cell premiums. Standard sales (category A), current view, all transfer
-- dates in the 12 months before the release (new builds register late, so
-- the window is kept wide; see 03).
WITH latest AS (
    SELECT strptime(max(release) || '-01', '%Y-%m-%d') AS release_start FROM changes
),
recent AS (
    SELECT p.*
    FROM price_paid_current AS p, latest AS l
    WHERE p.date_of_transfer >= l.release_start - INTERVAL 12 MONTH
      AND p.date_of_transfer <  l.release_start
      AND p.ppd_category = 'A'
      AND p.property_type IN ('D', 'S', 'T', 'F')
      AND p.postcode_district IS NOT NULL
),
cells AS (
    SELECT
        postcode_district,
        property_type,
        count(*) FILTER (WHERE new_build)          AS new_sales,
        count(*) FILTER (WHERE NOT new_build)      AS existing_sales,
        median(price) FILTER (WHERE new_build)     AS new_median,
        median(price) FILTER (WHERE NOT new_build) AS existing_median
    FROM recent
    GROUP BY ALL
),
matched AS (
    SELECT *, new_median / existing_median - 1 AS premium
    FROM cells
    WHERE new_sales >= 5 AND existing_sales >= 5
)
SELECT
    CASE property_type
        WHEN 'D' THEN 'Detached' WHEN 'S' THEN 'Semi-detached'
        WHEN 'T' THEN 'Terraced' ELSE 'Flat/maisonette' END AS property_type,
    count(*)                                        AS districts,
    sum(new_sales)::BIGINT                          AS new_sales,
    sum(existing_sales)::BIGINT                     AS existing_sales,
    round(100 * median(premium), 1)                 AS median_premium_pct,
    round(100 * quantile_cont(premium, 0.25), 1)    AS p25_premium_pct,
    round(100 * quantile_cont(premium, 0.75), 1)    AS p75_premium_pct,
    round(100.0 * avg((premium > 0)::INT), 1)       AS pct_districts_new_dearer
FROM matched
GROUP BY ALL
ORDER BY new_sales DESC;
