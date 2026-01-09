{{ config(
    materialized='table'
) }}

WITH dates AS (
    SELECT 
        d AS full_date
    FROM UNNEST(GENERATE_DATE_ARRAY('2006-01-01', '2007-12-01', INTERVAL 1 DAY)) AS d
),

-- Ajouter la clé technique
date_with_key AS (
    SELECT
       {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS id_date,  -- Clé technique
        full_date,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(WEEK FROM full_date) AS year_week,
        EXTRACT(DAY FROM full_date) AS year_day,
        EXTRACT(YEAR FROM full_date) AS fiscal_year,
        CAST(FLOOR((EXTRACT(MONTH FROM full_date)-1)/3 + 1) AS INT64) AS fiscal_qtr,
        EXTRACT(MONTH FROM full_date) AS month,
        FORMAT_DATE('%B', full_date) AS month_name,
        EXTRACT(DAYOFWEEK FROM full_date) AS week_day,   -- 1=Sunday ... 7=Saturday
        FORMAT_DATE('%A', full_date) AS day_name,
        CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) THEN 0 ELSE 1 END AS day_is_weekday
    FROM dates
)

SELECT *
FROM date_with_key
