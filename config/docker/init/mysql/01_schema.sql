-- Test schema for the MySQL source and target databases.
-- Loaded once by the mysql image when the data volume is empty.

CREATE DATABASE IF NOT EXISTS source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS target_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS source_db.orders (
    order_id    BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount      DECIMAL(12, 2) NOT NULL,
    status      VARCHAR(16) NOT NULL,
    city        VARCHAR(32) NOT NULL,
    order_time  DATETIME NOT NULL,
    updated_at  DATETIME NOT NULL,
    payload     VARCHAR(256) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_orders_updated_at (updated_at),
    KEY idx_orders_customer_id (customer_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS source_db.customers (
    customer_id BIGINT NOT NULL,
    name        VARCHAR(64) NOT NULL,
    city        VARCHAR(32) NOT NULL,
    segment     VARCHAR(16) NOT NULL,
    updated_at  DATETIME NOT NULL,
    payload     VARCHAR(256) NOT NULL,
    PRIMARY KEY (customer_id),
    KEY idx_customers_updated_at (updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS source_db.payments (
    payment_id   BIGINT NOT NULL,
    order_id     BIGINT NOT NULL,
    paid_amount  DECIMAL(12, 2) NOT NULL,
    payment_time DATETIME NOT NULL,
    updated_at   DATETIME NOT NULL,
    payload      VARCHAR(256) NOT NULL,
    PRIMARY KEY (payment_id),
    KEY idx_payments_order_id (order_id),
    KEY idx_payments_updated_at (updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS target_db.orders LIKE source_db.orders;
CREATE TABLE IF NOT EXISTS target_db.customers LIKE source_db.customers;
CREATE TABLE IF NOT EXISTS target_db.payments LIKE source_db.payments;

GRANT ALL PRIVILEGES ON target_db.* TO 'pudding'@'%';
FLUSH PRIVILEGES;

-- Debezium needs RELOAD/FLUSH_TABLES for its binlog-format check and
-- REPLICATION SLAVE/CLIENT to open the binlog stream.
GRANT RELOAD, FLUSH_TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pudding'@'%';
FLUSH PRIVILEGES;
