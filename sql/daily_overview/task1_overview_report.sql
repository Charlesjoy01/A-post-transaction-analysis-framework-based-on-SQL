USE test_database;
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET collation_connection = 'utf8mb4_0900_ai_ci';
SET @analysis_date = CURRENT_DATE();

WITH merchant_first_tx AS (
    SELECT merchant_id, DATE(MIN(transaction_time)) AS first_tx_date
    FROM transactions
    GROUP BY merchant_id
),
daily_metrics AS (
    SELECT 
        @analysis_date AS analysis_date,
        SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS total_volume_usd,
        COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS total_success_count,
        COUNT(*) AS total_transaction_count,
        CASE 
            WHEN COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) > 0 
            THEN SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) /
                 COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END)
            ELSE 0 
        END AS avg_transaction_value_usd,
        SUM(CASE WHEN mft.first_tx_date = @analysis_date AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS new_merchant_volume_usd,
        COUNT(DISTINCT CASE WHEN mft.first_tx_date = @analysis_date THEN t.merchant_id END) AS new_merchant_count,
        CASE 
            WHEN COUNT(CASE WHEN mft.first_tx_date = @analysis_date AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) > 0
            THEN SUM(CASE WHEN mft.first_tx_date = @analysis_date AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) /
                 COUNT(CASE WHEN mft.first_tx_date = @analysis_date AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END)
            ELSE 0
        END AS new_merchant_avg_value_usd,
        COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) * 100.0 / COUNT(*) AS success_rate_percent,
        COUNT(DISTINCT t.channel) AS active_channels_count,
        COUNT(DISTINCT t.merchant_id) AS active_merchants_count,
        COUNT(DISTINCT t.user_id) AS active_users_count
    FROM transactions t
    LEFT JOIN merchant_first_tx mft ON mft.merchant_id = t.merchant_id
    WHERE DATE(t.transaction_time) = @analysis_date
),
historical_comparison AS (
    SELECT 
        AVG(total_volume_usd) AS avg_7day_volume_usd,
        AVG(total_success_count) AS avg_7day_success_count,
        AVG(avg_transaction_value_usd) AS avg_7day_avg_value_usd,
        STDDEV(total_volume_usd) AS stddev_7day_volume_usd
    FROM (
        SELECT 
            DATE(t.transaction_time) AS transaction_date,
            SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS total_volume_usd,
            COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS total_success_count,
            AVG(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd END) AS avg_transaction_value_usd
        FROM transactions t
        WHERE DATE(t.transaction_time) BETWEEN DATE_SUB(@analysis_date, INTERVAL 7 DAY) AND DATE_SUB(@analysis_date, INTERVAL 1 DAY)
        GROUP BY DATE(t.transaction_time)
    ) daily_data
),
risk_indicators AS (
    SELECT 
        d.*,
        CASE 
            WHEN h.avg_7day_volume_usd > 0 
            THEN (d.total_volume_usd - h.avg_7day_volume_usd) * 100.0 / h.avg_7day_volume_usd
            ELSE 0
        END AS volume_volatility_percent,
        CASE 
            WHEN h.avg_7day_avg_value_usd > 0
            THEN (d.avg_transaction_value_usd - h.avg_7day_avg_value_usd) * 100.0 / h.avg_7day_avg_value_usd
            ELSE 0
        END AS avg_value_volatility_percent,
        CASE 
            WHEN ABS((d.total_volume_usd - h.avg_7day_volume_usd) * 100.0 / h.avg_7day_volume_usd) > 15
            THEN 'HIGH_VOLATILITY'
            WHEN d.success_rate_percent < 95
            THEN 'LOW_SUCCESS_RATE'
            WHEN d.new_merchant_avg_value_usd > d.avg_transaction_value_usd * 2
            THEN 'HIGH_NEW_MERCHANT_ACTIVITY'
            ELSE 'NORMAL'
        END AS risk_flag
    FROM daily_metrics d
    LEFT JOIN historical_comparison h ON 1=1
)
SELECT 
    analysis_date AS "Date",
    ROUND(total_volume_usd, 2) AS "Total Volume (USD)",
    total_success_count AS "Total Count",
    ROUND(avg_transaction_value_usd, 2) AS "Avg Transaction Value (USD)",
    ROUND(success_rate_percent, 2) AS "Success Rate (%)",
    new_merchant_count AS "New Merchant Count",
    ROUND(new_merchant_volume_usd, 2) AS "New Merchant Volume (USD)",
    ROUND(new_merchant_avg_value_usd, 2) AS "New Merchant Avg Value (USD)",
    active_channels_count AS "Active Channels",
    active_merchants_count AS "Active Merchants",
    active_users_count AS "Active Users",
    ROUND(volume_volatility_percent, 2) AS "Volume Volatility vs 7D Avg (%)",
    ROUND(avg_value_volatility_percent, 2) AS "Avg Value Volatility vs 7D Avg (%)",
    risk_flag AS "Risk Flag"
FROM risk_indicators
ORDER BY analysis_date DESC;

WITH channel_breakdown AS (
    SELECT 
        t.channel AS channel,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS volume_usd,
        COUNT(DISTINCT t.merchant_id) AS merchant_count,
        COUNT(DISTINCT t.user_id) AS user_count
    FROM transactions t
    WHERE DATE(t.transaction_time) = @analysis_date
    GROUP BY t.channel
)
SELECT 
    channel AS "Channel",
    transaction_count AS "Transaction Count",
    ROUND(volume_usd, 2) AS "Volume (USD)",
    merchant_count AS "Merchant Count",
    user_count AS "User Count",
    ROUND(volume_usd * 100.0 / SUM(volume_usd) OVER(), 2) AS "Volume Share (%)"
FROM channel_breakdown
ORDER BY volume_usd DESC;

WITH RECURSIVE month_days AS (
  SELECT DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01') AS day
  UNION ALL
  SELECT DATE_ADD(day, INTERVAL 1 DAY) FROM month_days
  WHERE day < LAST_DAY(CURRENT_DATE())
),
merchant_first_tx_month AS (
  SELECT merchant_id, DATE(MIN(transaction_time)) AS first_tx_date
  FROM transactions
  GROUP BY merchant_id
),
daily_calc AS (
  SELECT 
    DATE(t.transaction_time) AS day,
    SUM(CASE WHEN UPPER(t.status)='SUCCESS' THEN t.amount_usd ELSE 0 END) AS total_volume_usd,
    COUNT(CASE WHEN UPPER(t.status)='SUCCESS' THEN 1 END) AS total_success_count,
    COUNT(*) AS total_transaction_count,
    CASE WHEN COUNT(CASE WHEN UPPER(t.status)='SUCCESS' THEN 1 END)>0
      THEN SUM(CASE WHEN UPPER(t.status)='SUCCESS' THEN t.amount_usd ELSE 0 END) / COUNT(CASE WHEN UPPER(t.status)='SUCCESS' THEN 1 END)
      ELSE 0 END AS avg_transaction_value_usd,
    COUNT(DISTINCT CASE WHEN mft.first_tx_date=DATE(t.transaction_time) THEN t.merchant_id END) AS new_merchant_count,
    SUM(CASE WHEN mft.first_tx_date=DATE(t.transaction_time) AND UPPER(t.status)='SUCCESS' THEN t.amount_usd ELSE 0 END) AS new_merchant_volume_usd,
    CASE WHEN COUNT(CASE WHEN mft.first_tx_date=DATE(t.transaction_time) AND UPPER(t.status)='SUCCESS' THEN 1 END)>0
      THEN SUM(CASE WHEN mft.first_tx_date=DATE(t.transaction_time) AND UPPER(t.status)='SUCCESS' THEN t.amount_usd ELSE 0 END) / COUNT(CASE WHEN mft.first_tx_date=DATE(t.transaction_time) AND UPPER(t.status)='SUCCESS' THEN 1 END)
      ELSE 0 END AS new_merchant_avg_value_usd,
    COUNT(DISTINCT t.channel) AS active_channels_count,
    COUNT(DISTINCT t.merchant_id) AS active_merchants_count,
    COUNT(DISTINCT t.user_id) AS active_users_count,
    COUNT(CASE WHEN UPPER(t.status)='SUCCESS' THEN 1 END)*100.0/COUNT(*) AS success_rate_percent
  FROM transactions t
  LEFT JOIN merchant_first_tx_month mft ON mft.merchant_id=t.merchant_id
  WHERE DATE_FORMAT(t.transaction_time,'%Y-%m')=DATE_FORMAT(CURRENT_DATE(),'%Y-%m')
  GROUP BY DATE(t.transaction_time)
)
SELECT 
  md.day AS "Date",
  ROUND(IFNULL(dc.total_volume_usd,0),2) AS "Total Volume (USD)",
  IFNULL(dc.total_success_count,0) AS "Total Success Count",
  IFNULL(dc.total_transaction_count,0) AS "Total Transaction Count",
  ROUND(IFNULL(dc.avg_transaction_value_usd,0),2) AS "Avg Transaction Value (USD)",
  ROUND(IFNULL(dc.success_rate_percent,0),2) AS "Success Rate (%)",
  IFNULL(dc.new_merchant_count,0) AS "New Merchant Count",
  ROUND(IFNULL(dc.new_merchant_volume_usd,0),2) AS "New Merchant Volume (USD)",
  ROUND(IFNULL(dc.new_merchant_avg_value_usd,0),2) AS "New Merchant Avg Value (USD)",
  IFNULL(dc.active_channels_count,0) AS "Active Channels",
  IFNULL(dc.active_merchants_count,0) AS "Active Merchants",
  IFNULL(dc.active_users_count,0) AS "Active Users"
FROM month_days md
LEFT JOIN daily_calc dc ON dc.day=md.day
ORDER BY md.day DESC;
