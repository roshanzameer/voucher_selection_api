"""
Cleanse the raw data. Does pretty much same cleansing that is done in the EDA notebook
"""

import logging
import traceback
import pandas as pd

# from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


def parquet_to_df(parquet_file):
    logger.info('Reading the raw Parquet file')
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    return df


def str_to_int(col):
    if col == '' or col is None:
        return int(float(0))
    return int(float(col))


def clean_data(df):
    try:
        logger.info('Cleansing the raw Dataset')
        df['voucher_amount'] = df['voucher_amount'].fillna(0)
        df['total_orders'] = df['total_orders'].apply(lambda x: str_to_int(x))
        df['voucher_amount'] = df['voucher_amount'].apply(lambda x: str_to_int(x))
        for col in ["timestamp", "last_order_ts", "first_order_ts"]:
            df[col] = pd.to_datetime(df[col])
        logger.info('Done')
        return df
    except:
        traceback.print_exc()


def transform_load(parquet_file, clean_csv):
    dataframe = parquet_to_df(parquet_file)
    df = clean_data(dataframe)
    from airflow.hooks.postgres_hook import PostgresHook
    hook = PostgresHook('postgres_db')
    hook.autocommit = True
    df.to_sql(name='vouchers',
              index=False,
              chunksize=10000,
              method='multi',
              if_exists='append',
              con=hook.get_sqlalchemy_engine(),
              schema='public'
              )
    df.to_csv(clean_csv)
    logger.info('Data has been written to the DB')


if __name__ == '__main__':
    pass
