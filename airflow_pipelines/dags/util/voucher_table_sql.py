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


SEGMENT_SQL = """
            
            create table if not exists voucher_segments(
                country_code varchar,
                voucher_amount integer,
                recency_segment character varying,
                frequent_segment  character varying
            );
            
            insert into voucher_segments
            values 
                ('Peru', 0, '0-30', null),
                ('Peru', 0, '31-60', null),
                ('Peru', 0, '61-90', null),
                ('Peru', 0, '91-120', null),
                ('Peru', 0, '121-180', null),
                ('Peru', 0, '181+', null),
                ('Peru', 0, null, '0-4'),
                ('Peru', 0, null, '5-13'),
                ('Peru', 0, null, '14-37'),
                ('Peru', 0, null, '38+')

            """

with open(f'{dirpath}/voucher_table.sql', 'r') as sql:
    VOUCHER_SQL_DB = sql.read()
