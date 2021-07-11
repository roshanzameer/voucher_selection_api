import sys
from os.path import dirname, join, abspath
dirpath = dirname(abspath(__file__))


VOUCHER_SQL = """
            CREATE TABLE IF NOT EXISTS public.vouchers
            (
                id serial PRIMARY KEY,
                "timestamp" date,
                first_order_ts date,
                last_order_ts date,
                country_code character varying COLLATE pg_catalog."default",
                total_orders integer,
                voucher_amount integer
            )
            
            TABLESPACE pg_default;
            
            ALTER TABLE public.vouchers
                OWNER to postgres;
                
             """

with open(f'{dirpath}/voucher_table.sql', 'r') as sql:
    VOUCHER_SQL_DB = sql.read()
