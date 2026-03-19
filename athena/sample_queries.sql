-- Latest price

SELECT 
    symbol,
    close AS latest_price,
    window_end AS at
FROM gold_ohlcv
WHERE window_end = (SELECT MAX(window_end) FROM gold_ohlcv)
ORDER BY symbol;

-- Price difference at high and low 

SELECT
    symbol,
    MAX(high) AS session_high,
    MIN(low) AS session_low,
    MAX(high) - MIN(low) AS price_spread
FROM gold_ohlcv
GROUP BY symbol
ORDER BY price_spread DESC;

-- volume 

SELECT
  symbol,
  SUM(symbol) AS total_volume
FROM gold_ohlcv
GROUP BY symbol
ORDER BY total_volume DESC;

-- OHLCV profile

SELECT
  window_start,
  window_end,
  open,
  high,
  low,
  close,
  volume
FROM gold_ohlcv
WHERE symbol = 'AAPL'
ORDER BY  window_start;
