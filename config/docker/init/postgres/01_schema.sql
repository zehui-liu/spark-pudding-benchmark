-- Test schema for the PostgreSQL source and JDBC target databases.
-- Loaded once by the postgres image when the data volume is empty.

SELECT 'CREATE DATABASE target_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'target_db')
\gexec

CREATE TABLE IF NOT EXISTS orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);

CREATE TABLE IF NOT EXISTS customers (
    customer_id bigint PRIMARY KEY,
    name        varchar(64) NOT NULL,
    city        varchar(32) NOT NULL,
    segment     varchar(16) NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers (updated_at);

CREATE TABLE IF NOT EXISTS payments (
    payment_id   bigint PRIMARY KEY,
    order_id     bigint NOT NULL,
    paid_amount  numeric(12, 2) NOT NULL,
    payment_time timestamp NOT NULL,
    updated_at   timestamp NOT NULL,
    payload      varchar(256) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments (order_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at ON payments (updated_at);

\connect target_db

CREATE TABLE IF NOT EXISTS orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id bigint PRIMARY KEY,
    name        varchar(64) NOT NULL,
    city        varchar(32) NOT NULL,
    segment     varchar(16) NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id   bigint PRIMARY KEY,
    order_id     bigint NOT NULL,
    paid_amount  numeric(12, 2) NOT NULL,
    payment_time timestamp NOT NULL,
    updated_at   timestamp NOT NULL,
    payload      varchar(256) NOT NULL
);

-- Target of the E2E-04 incremental upsert case. Defined explicitly: SK-03 may
-- replace target_db.orders, and the upsert sink requires a primary key here.
CREATE TABLE IF NOT EXISTS e2e04_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Target of the E2E-08 streaming CDC case.
CREATE TABLE IF NOT EXISTS e2e08_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Target of the 10M-row E2E-04 scale run.
CREATE TABLE IF NOT EXISTS e2e04_scale_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Target of the RG-01 kill/recovery case.
CREATE TABLE IF NOT EXISTS rg01_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Targets of the RG-03 pending-transaction cases.
CREATE TABLE IF NOT EXISTS rg03_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS rg03_commit_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Target of the NG-06 streaming CDC negative cases.
CREATE TABLE IF NOT EXISTS ng06_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Source of the OS-05 state-backend cases (50 rows).
CREATE TABLE IF NOT EXISTS os05_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);

-- Target of the OS-03 HDFS state case.
CREATE TABLE IF NOT EXISTS os03_orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      varchar(16) NOT NULL,
    city        varchar(32) NOT NULL,
    order_time  timestamp NOT NULL,
    updated_at  timestamp NOT NULL,
    payload     varchar(256) NOT NULL
);
