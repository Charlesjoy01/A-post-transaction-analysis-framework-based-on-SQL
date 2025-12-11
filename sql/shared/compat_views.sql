USE test_database;
CREATE OR REPLACE VIEW transactions_payin AS
SELECT
  DATE(ods.busi_date) AS transaction_date,
  ods.merchant_no AS merchant_id,
  ods.user_unique_identification AS user_id,
  ods.channel AS channel,
  ods.create_time AS transaction_time,
  UPPER(COALESCE(ods.channel_order_status, ods.payrespinfo_code)) AS status,
  CASE WHEN UPPER(COALESCE(ods.channel_order_status, ods.payrespinfo_code)) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN
    CASE WHEN UPPER(ods.pay_currency) = 'USD' THEN ods.pay_amount ELSE ods.pay_amount * fr.to_usd_rate END
  ELSE 0 END AS amount_usd,
  'payin' AS transaction_type
FROM ods_pagsmile_orders_raw ods
LEFT JOIN fx_rates fr ON UPPER(fr.currency_code) = UPPER(ods.pay_currency) AND fr.rate_date = DATE(ods.busi_date);

CREATE OR REPLACE VIEW transactions_payout AS
SELECT
  DATE(ods.busi_date) AS transaction_date,
  ods.merchant_no AS merchant_id,
  ods.user_unique_identification AS user_id,
  ods.thirdparty_channel AS channel,
  ods.create_time AS transaction_time,
  UPPER(ods.transaction_status) AS status,
  CASE WHEN UPPER(ods.transaction_status) IN ('SUCCESS','PAID') THEN
    CASE WHEN UPPER(ods.payout_currency) = 'USD' THEN ods.amount ELSE ods.amount * fr.to_usd_rate END
  ELSE 0 END AS amount_usd,
  'payout' AS transaction_type
FROM ods_transfersmile_payouts_raw ods
LEFT JOIN fx_rates fr ON UPPER(fr.currency_code) = UPPER(ods.payout_currency) AND fr.rate_date = DATE(ods.create_time);

CREATE OR REPLACE VIEW transactions AS
SELECT * FROM transactions_payin
UNION ALL
SELECT * FROM transactions_payout;

CREATE OR REPLACE VIEW merchants AS
SELECT merchant_no AS merchant_id, MAX(merchant_name) AS merchant_name, MIN(DATE(create_time)) AS join_date, 'active' AS status, NULL AS industry, NULL AS risk_level, NULL AS business_type, NULL AS registered_country
FROM (
  SELECT merchant_no, merchant_name, create_time FROM ods_pagsmile_orders_raw
  UNION ALL
  SELECT merchant_no, merchant_name, create_time FROM ods_transfersmile_payouts_raw
) m
GROUP BY merchant_no;

CREATE OR REPLACE VIEW users AS
SELECT user_id, MAX(user_name) AS user_name, MIN(DATE(first_seen)) AS registration_date, 'active' AS status, NULL AS risk_status
FROM (
  SELECT user_unique_identification AS user_id, NULL AS user_name, create_time AS first_seen FROM ods_pagsmile_orders_raw
  UNION ALL
  SELECT user_unique_identification AS user_id, NULL AS user_name, create_time AS first_seen FROM ods_transfersmile_payouts_raw
) u
GROUP BY user_id;
