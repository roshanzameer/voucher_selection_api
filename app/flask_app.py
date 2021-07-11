import sys
import logging
import traceback

from flask_cors import CORS
from flask import Flask, request, jsonify
from db_config.connector import get_connection
from get_segments import calculate_voucher_amount

logger = logging.getLogger('app')

connection, cursor = get_connection()

app = Flask(__name__)
CORS(app)


@app.route('/health', methods=['GET'])
def health_check():
    """
    Perform Health check. Connects to a DB and returns connection status
    :return: status JSON
    """
    try:
        cursor.execute('SELECT VERSION()')
        db_version = cursor.fetchone()
        return jsonify({'status': True, 'db': db_version})
    except Exception as e:
        print(e)


@app.route('/voucher', methods=['POST'])
def voucher_selection():
    """
    Returns voucher amount querying from a DB
    :return: Object
    """
    try:
        request_payload = request.get_json()
        last_order_ts = request_payload.get('last_order_ts')
        first_order_ts = request_payload.get('first_order_ts')
        total_orders = request_payload.get('total_orders')
        segment_name = request_payload.get('segment_name')
        voucher_amount = calculate_voucher_amount(first_order=first_order_ts,
                                                  last_order=last_order_ts,
                                                  total_orders=total_orders,
                                                  segment_type=segment_name)
        if not voucher_amount:
            return jsonify({'voucher_amount': 0})

        return jsonify({'voucher_amount': voucher_amount})

    except Exception as e:
        logger.error(f'{traceback.format_exc()}')
        return jsonify({'status': False}), 500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8000)
