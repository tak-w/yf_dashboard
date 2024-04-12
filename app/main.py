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

# print(dfs_dict)
bq.load_data_to_bq(dfs_dict)
