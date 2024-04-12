import yfinance as yf

symbols_dict = {
    'dow': '^DJI',
    'sp500': '^SPX',
    'nasdaq': '^IXIC',
    'vym': 'VYM',
    'nikkei': '^N225',  # JPY
    'shanghai': '000001.SS',  # CNY
    'btc_usd': 'BTC-USD',
    # btcjpy must be calculated from btcusd and usdjpy
    'usd_jpy': 'JPY=X',  # JPY
    'eur_usd': 'EURUSD=X',
    'dxy': 'DX-Y.NYB',  # ICE US Dollar Index
    'nvda': 'NVDA',
    'aapl': 'AAPL',
    'gold': 'GC=F',
    'silver': 'SI=F',
    # gold_silver_ratio must be calculated from gold and silver
    'copper': 'HG=F',
    'platinum': 'PL=F',
    'crude_oil': 'CL=F',
    'brent_oil': 'BZ=F',
    'commodity_index': 'GD=F',
    'vix': '^VIX',
    'us_yield_5y': '^FVX',
    'us_yield_10y': '^TNX',
    'us_yield_30y': '^TYX',
    'tlt': 'TLT',
}

dfs_dict = symbols_dict.copy()
for k in dfs_dict:
    dfs_dict[k] = None  # Values should be replaced to dataframes later

# Some ETFs have a column 'Capital Gains' that should be removed
columns_ls = [
    'Date',
    'Open',
    'High',
    'Low',
    'Close',
    'Volume',
    'Dividends',
    'Stock Splits',
]


def fecth_yf(symbols_dict):
    for k, v in symbols_dict.items():
        ticker = yf.Ticker(v)
        df = ticker.history(period='7d')
        # Drop columns unmatch with columns_ls
        for c in df.columns.values:
            if c not in columns_ls:
                df.drop(columns=c, inplace=True)
        df.index = df.index.tz_convert('UTC')
        dfs_dict[k] = df
    return dfs_dict


def validate_dfs_dict(dfs_dict):
    for k, v in dfs_dict.items():
        if v is None:
            raise TypeError(f'dfs_dict has None values. key: {k}, value: {v}')
    return dfs_dict


if __name__ == '__main__':
    dfs_dict = fecth_yf(symbols_dict)
    print(validate_dfs_dict(dfs_dict))
