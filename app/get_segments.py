# from app_config import segments
import datetime
from dateutil.parser import parse

# connection, cursor = get_connection()

segments = {
    'frequent_segment': {
        '0-4': {'lower': 0, 'upper': 4},
        '5-13': {'lower': 5, 'upper': 13},
        '14-37': {'lower': 14, 'upper': 37},
        '38+': {'lower': 38, 'upper': 10000},
    },
    'recency_segment': {
        '0-30': {'lower': 0, 'upper': 30},
        '31-60': {'lower': 31, 'upper': 60},
        '61-90': {'lower': 61, 'upper': 90},
        '91-120': {'lower': 91, 'upper': 120},
        '121-180': {'lower': 121, 'upper': 180},
        '181+': {'lower': 181, 'upper': 10000},
    }
}


def find_segment(num, segment_type):
    for key, value in segments[segment_type].items():
        if value['lower'] <= num <= value['upper']:
            return key
        else:
            continue
    raise ValueError('out-of-bound error')


def calculate_voucher_amount(first_order, last_order, total_orders, segment_type):
    from flask_app import cursor
    last_order = parse(last_order)
    first_order = parse(first_order)
    segment = ''
    if segment_type == 'recency_segment':
        duration = (last_order - first_order).days
        segment = find_segment(duration, segment_type)
    elif segment_type == 'frequent_segment':
        segment = find_segment(total_orders, segment_type)
    query = f"SELECT voucher_amount FROM public.voucher_segments WHERE {segment_type}='{segment}'"
    cursor.execute(query)
    amount = cursor.fetchone()[0]
    return amount


if __name__ == '__main__':
    input = {
        "customer_id": 123,
        "country_code": "Peru",
        "last_order_ts": "2018-05-03 00:00:00",
        "first_order_ts": "2017-05-03 00:00:00",
        "total_orders": 3,
        "segment_name": "frequent_segment"
        }

    print(calculate_voucher_amount("2017-05-03 00:00:00", "2018-05-03 00:00:00", 15, 'frequent_segment'))

