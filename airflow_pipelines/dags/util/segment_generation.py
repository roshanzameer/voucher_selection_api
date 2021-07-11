# from app_config import segments
import traceback

from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

segments = [
    {
        'type': 'recency_segment',
        'range': '0-30',
        'lower': 0,
        'upper': 30
    },
    {
        'type': 'recency_segment',
        'range': '31-60',
        'lower': 31,
        'upper': 60
    },
    {
        'type': 'recency_segment',
        'range': '61-90',
        'lower': 61,
        'upper': 90
    },
    {
        'type': 'recency_segment',
        'range': '91-120',
        'lower': 91,
        'upper': 120
    },
    {
        'type': 'recency_segment',
        'range': '121-180',
        'lower': 121,
        'upper': 180
    },
    {
        'type': 'recency_segment',
        'range': '180+',
        'lower': 180,
        'upper': 10000
    },
    {
        'type': 'frequent_segment',
        'range': '0-4',
        'lower': 0,
        'upper': 4
    },
    {
        'type': 'frequent_segment',
        'range': '5-13',
        'lower': 5,
        'upper': 13
    },
    {
        'type': 'frequent_segment',
        'range': '14-37',
        'lower': 14,
        'upper': 37
    },
    {
        'type': 'frequent_segment',
        'range': '38+',
        'lower': 38,
        'upper': 10000
    }
]


def segment_db(amount, segment_type, segment_name):
    query = f'''
        update public.voucher_segments
        set voucher_amount = {int(amount)}
        where {segment_type} = '{segment_name}'
    '''
    hook = PostgresHook('postgres_db')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()


def frequency_voucher_amount(df, lower, upper):
    df_segment = df[(df['total_orders'].astype(float) >= lower)
                    & (df['total_orders'].astype(float) <= upper)]

    amount = df_segment.groupby(['voucher_amount']).count().total_orders.idxmax()
    return amount


def recency_voucher_amount(df, lower_day, upper_day):
    df_time_delta = df[
        ((pd.to_datetime(df['last_order_ts']) - pd.to_datetime(df['first_order_ts'])) >= timedelta(lower_day))
        & ((pd.to_datetime(df['last_order_ts']) - pd.to_datetime(df['first_order_ts'])) <= timedelta(upper_day))]
    amount = df_time_delta.groupby(['voucher_amount']).count().total_orders.idxmax()
    return amount


def generate_segments(clean_csv):
    df = pd.read_csv(clean_csv)
    df = df[df['country_code'] == 'Peru']
    amount = 0
    for segment in segments:
        try:
            if segment['type'] == 'recency_segment':
                amount = recency_voucher_amount(df, segment['lower'], segment['upper'])
            elif segment['type'] == 'frequent_segment':
                amount = frequency_voucher_amount(df, segment['lower'], segment['upper'])

            segment_db(amount, segment['type'], segment['range'])
        except Exception as e:
            logger.error('Failed to insert segments to DB', e)
            logger.error(f'{traceback.format_exc()}')

    logger.info('Segments written to the DB')
