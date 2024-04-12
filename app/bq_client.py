from google.cloud import bigquery
import yf_client as yf


client = bigquery.Client()


def load_data_to_bq(dfs_dict):
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('date', bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField('open', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('high', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('low', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('close', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('volume', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('dividends', bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField('stock_splits', bigquery.enums.SqlTypeNames.FLOAT64),
        ],
        write_disposition='WRITE_TRUNCATE',  # Equivalent to 'CREATE OR REPLACE TABLE'
    )
    for k, v in dfs_dict.items():
        dataframe = v.rename(columns=lambda s: s.lower().replace(' ', '_'))
        dataframe.index.name = 'date'
        table_id = f'yf-dashboard.temp_int_1d.temp_{k}'
        # print(dataframe)
        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}'
        )


def merge_temp_to_hist():
    symbols_ls = yf.symbols_dict.keys()
    for k in symbols_ls:
        query = f"""
        MERGE `yf-dashboard.int_1d.{k}` as t
        USING `yf-dashboard.temp_int_1d.temp_{k}` as s
        ON t.date = s.date
        WHEN MATCHED THEN
        UPDATE SET
            t.date = s.date,
            t.open = s.open,
            t.high = s.high,
            t.low = s.low,
            t.close = s.close,
            t.volume = s.volume,
            t.dividends = s.dividends,
            t.stock_splits = s.stock_splits
        WHEN NOT MATCHED THEN
        INSERT (date, open, high, low, close, volume, dividends, stock_splits)
        VALUES (s.date, s.open, s.high, s.low, s.close, s.volume, s.dividends, s.stock_splits)
        """
        query_job = client.query(
            query,
            location='asia-northeast1',
            job_config=bigquery.QueryJobConfig(
                labels={'exec-label': 'python-client-library'}
            ),
        )
        print(f'Started job: {query_job.job_id}')


if __name__ == '__main__':
    merge_temp_to_hist()
