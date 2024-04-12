import yf_client as yf
import bq_client as bq


symbols_dict = yf.symbols_dict
dfs_dict = yf.dfs_dict
columns_ls = yf.columns_ls


dfs_dict = yf.fecth_yf(symbols_dict)
try:
    dfs_dict = yf.validate_dfs_dict(dfs_dict)
except TypeError as e:
    print(f'ERROR: {e}')

bq.load_data_to_bq(dfs_dict)
bq.merge_temp_to_hist()
print('Loading data from yfinance to BigQuery has successfully completed!')
