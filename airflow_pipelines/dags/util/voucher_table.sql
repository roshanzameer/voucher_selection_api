--
-- PostgreSQL database dump
--

CREATE TABLE IF NOT EXISTS public.vouchers
(
    id integer NOT NULL DEFAULT nextval('vouchers_id_seq'::regclass),
    "timestamp" date,
    first_order_ts date,
    last_order_ts date,
    country_code character varying COLLATE pg_catalog."default",
    total_orders integer,
    voucher_amount integer,
    CONSTRAINT vouchers_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public.vouchers
    OWNER to postgres;
