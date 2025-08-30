# Data Schema (Unified)
All time series are UTC-based. Provide both UTC and KST columns where applicable for reporting convenience.

## Candles (1m baseline)
- open_time (int or ISO8601 UTC)
- open (float)
- high (float)
- low (float)
- close (float)
- volume (float)

## Optional Extended Columns
- symbol, exchange, trade_count, taker_buy_volume, taker_buy_quote, funding_rate, mark_price

## Validators
1. Non-decreasing timestamps
2. Finite numeric values (not NaN/Inf)
3. High/Low bounds: low <= open,close <= high
4. Volume >= 0
