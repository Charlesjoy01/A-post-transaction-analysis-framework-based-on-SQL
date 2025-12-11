USE test_database;

CREATE TABLE IF NOT EXISTS ods_pagsmile_orders_raw (
  busi_date DATETIME NOT NULL,
  trade_no VARCHAR(64) NOT NULL,
  merchant_no VARCHAR(64) NOT NULL,
  merchant_name VARCHAR(256),
  user_unique_identification VARCHAR(128),
  channel VARCHAR(64),
  create_time DATETIME NOT NULL,
  channel_order_status VARCHAR(64),
  payrespinfo_code VARCHAR(64),
  pay_currency VARCHAR(16),
  pay_amount DECIMAL(18,8),
  PRIMARY KEY (trade_no)
);

CREATE TABLE IF NOT EXISTS ods_transfersmile_payouts_raw (
  busi_date DATETIME NOT NULL,
  transaction_id VARCHAR(64) NOT NULL,
  merchant_no VARCHAR(64) NOT NULL,
  merchant_name VARCHAR(256),
  user_unique_identification VARCHAR(128),
  thirdparty_channel VARCHAR(64),
  create_time DATETIME NOT NULL,
  transaction_status VARCHAR(64),
  payout_currency VARCHAR(16),
  amount DECIMAL(18,8),
  PRIMARY KEY (transaction_id)
);
