MERGE `yf-dashboard.int_1d.{k}` as t USING `yf-dashboard.temp_int_1d.temp_{k}` as s ON t.date = s.date WHEN MATCHED THEN
UPDATE
SET
  t.date = s.date,
  t.open = s.open,
  t.high = s.high,
  t.low = s.low,
  t.close = s.close,
  t.volume = s.volume,
  t.dividends = s.dividends,
  t.stock_splits = s.stock_splits WHEN NOT MATCHED THEN INSERT (
    date,
    open,
    high,
    low,
    close,
    volume,
    dividends,
    stock_splits
  )
VALUES
  (
    s.date,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.dividends,
    s.stock_splits
  )