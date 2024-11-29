WITH today AS (
    SELECT * FROM {{var.value.SCHEMA}}.{{var.value.STAGING_TABLE}}
    WHERE date = DATE('{{var.value.RUN_DATE}}')
    ),
    last_week AS (
    SELECT * FROM {{var.value.SCHEMA}}.{{var.value.PROD_TABLE}}
    WHERE date = DATE('{{var.value.RUN_DATE}}') - INTERVAL '7 days'
    ),
    aggregate AS (

    SELECT t.ticker,
            COUNT(1) > 1 as has_duplicates,
            COUNT(CASE WHEN t.close/l.close > 1.1 OR t.close/l.close < 0.9 THEN 1 END) > 0 AS has_big_change
        FROM today t FULL OUTER JOIN last_week l
        ON t.ticker = l.ticker
    GROUP BY t.ticker
    )

    SELECT * FROM aggregate WHERE has_duplicates or has_big_change