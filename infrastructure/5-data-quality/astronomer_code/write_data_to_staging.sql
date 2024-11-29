INSERT INTO {{var.value.SCHEMA}}.{{var.value.STAGING_TABLE}} (date, ticker, high, low, open, close, volume)
SELECT date, ticker, high, low, open, close, volume
FROM {{fetch_stock_data}}