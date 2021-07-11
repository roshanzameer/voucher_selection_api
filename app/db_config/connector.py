import os
import atexit
import psycopg2
from db_creds import LOCAL, table_config
import logging
log = logging.getLogger(__name__)

ENV = os.environ['ENV']

connection = None
cursor = None


class PgConnector:

    @staticmethod
    def create_pgsql_connection():
        """
        Create psycopg2 connection string based on the Database operation environment
        :return: Connection Sting: Object
        """

        if ENV == 'docker':
            host = os.environ['HOST']
            port = os.environ['PORT']
            username = os.environ['USERNAME']
            password = os.environ['PASSWORD']
            database = os.environ['DATABASE']
            options = os.environ['OPTIONS']

            connection = psycopg2.connect(user=username,
                                          password=password,
                                          host=host,
                                          port=port,
                                          options=options,
                                          database=database)
            return connection


def get_connection():
    """
    To efficiently reuse the Postgres connection pool
    :return: global connection and cursor objects: Object
    """
    global connection
    global cursor
    if not connection:
        try:
            connection = PgConnector.create_pgsql_connection()
            cursor = connection.cursor()
            cursor.execute('SELECT VERSION()')
            db_version = cursor.fetchone()
            log.info("Connected to DB: version:{}".format(db_version))
        except Exception as error:
            print('Error connecting to Database {}'.format(error))
            connection = None
            cursor = None

    #atexit.register(close_connection, connection, cursor)
    return connection, cursor


def close_connection(conn, cur):
    """
    To automatically terminate Database connection when the Flask service exits
    :param conn: global connection string: Object
    :param cur: global connection cursor: Object
    :return: None
    """
    if conn and cur:
        cur.close()
        conn.close()
        log.info("Connection to DB Terminated")
