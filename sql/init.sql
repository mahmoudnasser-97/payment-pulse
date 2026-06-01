-- PaymentPulse Data Warehouse Schema

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      VARCHAR(36) PRIMARY KEY,
    event_time          TIMESTAMP NOT NULL,
    customer_id         VARCHAR(36),
    merchant_id         VARCHAR(36),
    service_type        VARCHAR(50),
    amount_egp          NUMERIC(12, 2),
    governorate         VARCHAR(50),
    payment_method      VARCHAR(30),
    status              VARCHAR(20),
    is_fraud            BOOLEAN DEFAULT FALSE,
    fraud_score         NUMERIC(5, 4),
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS merchant_daily_summary (
    summary_date        DATE NOT NULL,
    merchant_id         VARCHAR(36) NOT NULL,
    service_type        VARCHAR(50),
    total_transactions  INTEGER,
    total_amount_egp    NUMERIC(14, 2),
    fraud_count         INTEGER,
    PRIMARY KEY (summary_date, merchant_id)
);

CREATE TABLE IF NOT EXISTS governorate_daily_summary (
    summary_date        DATE NOT NULL,
    governorate         VARCHAR(50) NOT NULL,
    total_transactions  INTEGER,
    total_amount_egp    NUMERIC(14, 2),
    fraud_count         INTEGER,
    PRIMARY KEY (summary_date, governorate)
);