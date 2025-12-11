USE test_database;
CREATE OR REPLACE VIEW output_overview_board AS
WITH RECURSIVE one_row AS (
    SELECT 1 AS x
),
bounds AS (
    SELECT 
        COALESCE(rmc.first_day, DATE(CONCAT(YEAR(CURRENT_DATE()), '-', LPAD(MONTH(CURRENT_DATE()), 2, '0'), '-01'))) AS first_day,
        COALESCE(rmc.last_day, LAST_DAY(DATE(CONCAT(YEAR(CURRENT_DATE()), '-', LPAD(MONTH(CURRENT_DATE()), 2, '0'), '-01')))) AS last_day
    FROM one_row
    LEFT JOIN (
        SELECT first_day, last_day 
        FROM report_month_config 
        ORDER BY updated_at DESC, id DESC 
        LIMIT 1
    ) rmc ON 1=1
),
calendar AS (
    SELECT first_day AS d FROM bounds
    UNION ALL
    SELECT DATE_ADD(d, INTERVAL 1 DAY) FROM calendar JOIN bounds WHERE d < bounds.last_day
),
merchant_first_tx AS (
    SELECT merchant_id, DATE(MIN(transaction_time)) AS first_tx_date
    FROM transactions
    GROUP BY merchant_id
),
daily_summary AS (
    SELECT 
        cal.d AS analysis_date,
        SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS total_volume_usd,
        COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS total_success_count,
        COUNT(t.transaction_time) AS total_transaction_count,
        CASE 
            WHEN COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) > 0 
            THEN SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) /
                 COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END)
            ELSE 0 
        END AS avg_transaction_value_usd,
        COUNT(DISTINCT CASE WHEN mft.first_tx_date = cal.d THEN t.merchant_id END) AS new_merchant_count,
        SUM(CASE WHEN mft.first_tx_date = cal.d AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS new_merchant_volume_usd,
        CASE 
            WHEN COUNT(CASE WHEN mft.first_tx_date = cal.d AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) > 0
            THEN SUM(CASE WHEN mft.first_tx_date = cal.d AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) /
                 COUNT(CASE WHEN mft.first_tx_date = cal.d AND UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END)
            ELSE 0
        END AS new_merchant_avg_value_usd,
        COUNT(DISTINCT t.channel) AS active_channels_count,
        COUNT(DISTINCT t.merchant_id) AS active_merchants_count,
        COUNT(DISTINCT t.user_id) AS active_users_count,
        CASE 
            WHEN COUNT(t.transaction_time) > 0 
            THEN COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) * 100.0 / COUNT(t.transaction_time)
            ELSE 0
        END AS success_rate_percent
    FROM calendar cal
    LEFT JOIN transactions t ON DATE(t.transaction_time) = cal.d
    LEFT JOIN merchant_first_tx mft ON mft.merchant_id = t.merchant_id
    GROUP BY cal.d
),
historical_comparison AS (
    SELECT 
        cal.d AS analysis_date,
        AVG(dd.total_volume_usd) AS avg_7day_volume_usd,
        AVG(dd.total_success_count) AS avg_7day_success_count,
        AVG(dd.avg_transaction_value_usd) AS avg_7day_avg_value_usd,
        STDDEV(dd.total_volume_usd) AS stddev_7day_volume_usd
    FROM calendar cal
    LEFT JOIN (
        SELECT 
            DATE(t.transaction_time) AS transaction_date,
            SUM(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS total_volume_usd,
            COUNT(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS total_success_count,
            AVG(CASE WHEN UPPER(t.status) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd END) AS avg_transaction_value_usd
        FROM transactions t
        GROUP BY DATE(t.transaction_time)
    ) dd
    ON dd.transaction_date BETWEEN DATE_SUB(cal.d, INTERVAL 7 DAY) AND DATE_SUB(cal.d, INTERVAL 1 DAY)
    GROUP BY cal.d
),
risk_flags AS (
    SELECT 
        ds.analysis_date,
        'HIGH_VOLATILITY' AS flag_type, 
        'Volume volatility > 15% vs 7-day average' AS description,
        CASE WHEN hc.avg_7day_volume_usd > 0 AND ABS((ds.total_volume_usd - hc.avg_7day_volume_usd) * 100.0 / hc.avg_7day_volume_usd) > 15 THEN 1 ELSE 0 END AS is_flagged
    FROM daily_summary ds
    LEFT JOIN historical_comparison hc ON hc.analysis_date = ds.analysis_date
    UNION ALL
    SELECT 
        ds.analysis_date,
        'LOW_SUCCESS_RATE', 
        'Success rate < 95%', 
        CASE WHEN ds.success_rate_percent < 95 THEN 1 ELSE 0 END
    FROM daily_summary ds
    UNION ALL
    SELECT 
        ds.analysis_date,
        'HIGH_NEW_MERCHANT_ACTIVITY', 
        'New merchant avg transaction > 2x overall average', 
        CASE WHEN ds.new_merchant_avg_value_usd > ds.avg_transaction_value_usd * 2 THEN 1 ELSE 0 END
    FROM daily_summary ds
)
SELECT 
    ds.analysis_date AS "Date",
    ROUND(ds.total_volume_usd, 2) AS "Total Volume (USD)",
    ds.total_success_count AS "Total Success Count",
    ds.total_transaction_count AS "Total Transaction Count",
    ROUND(ds.avg_transaction_value_usd, 2) AS "Avg Transaction Value (USD)",
    ROUND(ds.success_rate_percent, 2) AS "Success Rate (%)",
    ds.new_merchant_count AS "New Merchant Count",
    ROUND(ds.new_merchant_volume_usd, 2) AS "New Merchant Volume (USD)",
    ROUND(ds.new_merchant_avg_value_usd, 2) AS "New Merchant Avg Value (USD)",
    ds.active_channels_count AS "Active Channels",
    ds.active_merchants_count AS "Active Merchants",
    ds.active_users_count AS "Active Users",
    ROUND(hc.avg_7day_volume_usd, 2) AS "7-Day Avg Volume (USD)",
    ROUND(hc.avg_7day_success_count, 0) AS "7-Day Avg Success Count",
    ROUND(hc.avg_7day_avg_value_usd, 2) AS "7-Day Avg Transaction Value (USD)",
    ROUND((ds.total_volume_usd - hc.avg_7day_volume_usd) * 100.0 / NULLIF(hc.avg_7day_volume_usd, 0), 2) AS "Volume Volatility (%)",
    ROUND((ds.avg_transaction_value_usd - hc.avg_7day_avg_value_usd) * 100.0 / NULLIF(hc.avg_7day_avg_value_usd, 0), 2) AS "Avg Value Volatility (%)",
    GROUP_CONCAT(CASE WHEN rf.is_flagged = 1 THEN rf.flag_type END ORDER BY rf.flag_type SEPARATOR ', ') AS "Risk Flags",
    CASE 
        WHEN SUM(CASE WHEN rf.is_flagged = 1 THEN 1 ELSE 0 END) = 0 THEN 'NORMAL'
        WHEN SUM(CASE WHEN rf.is_flagged = 1 THEN 1 ELSE 0 END) = 1 THEN 'CAUTION'
        ELSE 'ALERT'
    END AS "Overall Status",
    CURRENT_TIMESTAMP() AS "Last Updated"
FROM daily_summary ds
LEFT JOIN historical_comparison hc ON hc.analysis_date = ds.analysis_date
LEFT JOIN risk_flags rf ON rf.analysis_date = ds.analysis_date
GROUP BY ds.analysis_date, ds.total_volume_usd, ds.total_success_count, ds.total_transaction_count, 
         ds.avg_transaction_value_usd, ds.success_rate_percent, ds.new_merchant_count, ds.new_merchant_volume_usd, ds.new_merchant_avg_value_usd,
         ds.active_channels_count, ds.active_merchants_count, ds.active_users_count,
         hc.avg_7day_volume_usd, hc.avg_7day_success_count, hc.avg_7day_avg_value_usd;
