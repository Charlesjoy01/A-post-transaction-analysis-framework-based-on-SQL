USE test_database;
-- SQL任务4：每日随机抽检（关键任务）
-- 目标：弥补Top N分析的盲区，验证商户性质，评估洗钱风险

-- 设置查询参数
SET @analysis_date = CURRENT_DATE();
SET @sample_size = 10;  -- 随机抽取10个商户
SET @large_transaction_threshold = 3.0;  -- 超过笔均300%的阈值

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_randomly_selected_merchants AS
SELECT merchant_id, merchant_name, recent_7day_count, recent_7day_volume_usd
FROM (
    SELECT 
        t.`MERCHANT ID` AS merchant_id,
        MAX(t.`MERCHANT NAME`) AS merchant_name,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 1 END) AS recent_7day_count,
        SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
        ELSE 0 END) AS recent_7day_volume_usd,
        ROW_NUMBER() OVER(
            ORDER BY 
                MOD(ABS(CRC32(CAST(t.`MERCHANT ID` AS CHAR))) + UNIX_TIMESTAMP(@analysis_date), 1000000),
                t.`MERCHANT ID`
        ) AS random_rank
    FROM tables t
    LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
    WHERE DATE(t.`PAY TIME`) BETWEEN DATE_SUB(@analysis_date, INTERVAL 7 DAY) AND @analysis_date
    GROUP BY t.`MERCHANT ID`
    HAVING COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 1 END) >= 5
) ms
WHERE random_rank <= @sample_size;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_merchant_analysis AS
SELECT 
    rsm.merchant_id,
    rsm.merchant_name,
    rsm.recent_7day_count,
    rsm.recent_7day_volume_usd,
    rsm.recent_7day_volume_usd / NULLIF(rsm.recent_7day_count, 0) AS avg_transaction_value_usd,
    m.med AS median_transaction_value_usd,
    AVG(tx.amount_usd) AS mean_transaction_value_usd,
    STDDEV(tx.amount_usd) AS stddev_transaction_value_usd,
    COUNT(CASE WHEN tx.amount_usd > (rsm.recent_7day_volume_usd / NULLIF(rsm.recent_7day_count, 0)) * @large_transaction_threshold THEN 1 END) AS large_transaction_count,
    COUNT(CASE WHEN tx.amount_usd > (rsm.recent_7day_volume_usd / NULLIF(rsm.recent_7day_count, 0)) * @large_transaction_threshold THEN 1 END) * 100.0 / COUNT(*) AS large_transaction_ratio_percent,
    COUNT(DISTINCT tx.channel) AS unique_channels,
    COUNT(CASE WHEN HOUR(tx.transaction_time) BETWEEN 0 AND 6 THEN 1 END) AS off_hours_count,
    COUNT(CASE WHEN DAYOFWEEK(tx.transaction_date) IN (1, 7) THEN 1 END) AS weekend_count,
    0 AS payin_count,
    0 AS payout_count,
    0.0 AS payin_volume_usd,
    0.0 AS payout_volume_usd,
    NULL AS industry,
    NULL AS risk_level,
    NULL AS join_date
FROM tmp_randomly_selected_merchants rsm
LEFT JOIN (
    SELECT 
        t.`MERCHANT ID` AS merchant_id,
        CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END AS amount_usd,
        t.`PAY CHANNEL` AS channel,
        t.`PAY TIME` AS transaction_time,
        DATE(t.`PAY TIME`) AS transaction_date
    FROM tables t
    LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
    WHERE DATE(t.`PAY TIME`) BETWEEN DATE_SUB(@analysis_date, INTERVAL 7 DAY) AND @analysis_date
      AND UPPER(t.`STATUS`) = 'SUCCESS'
) tx ON tx.merchant_id = rsm.merchant_id
LEFT JOIN (
    SELECT merchant_id,
           AVG(CASE WHEN rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2)) THEN amount_usd END) AS med
    FROM (
        SELECT 
            t.`MERCHANT ID` AS merchant_id,
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END AS amount_usd,
            ROW_NUMBER() OVER (PARTITION BY t.`MERCHANT ID` ORDER BY CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END) AS rn,
            COUNT(*) OVER (PARTITION BY t.`MERCHANT ID`) AS cnt
        FROM tables t
        LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
        WHERE DATE(t.`PAY TIME`) BETWEEN DATE_SUB(@analysis_date, INTERVAL 7 DAY) AND @analysis_date
          AND UPPER(t.`STATUS`) = 'SUCCESS'
    ) s
    GROUP BY merchant_id
) m ON m.merchant_id = rsm.merchant_id
GROUP BY rsm.merchant_id, rsm.merchant_name, rsm.recent_7day_count, rsm.recent_7day_volume_usd, m.med;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_top_user_analysis AS
SELECT 
    ma.merchant_id,
    ma.merchant_name,
    t.`USER ID` AS user_id,
    MAX(t.`USER NAME`) AS user_name,
    NULL AS risk_status,
    COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 1 END) AS user_transaction_count,
    SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END ELSE 0 END) AS user_volume_usd,
    RANK() OVER(PARTITION BY ma.merchant_id ORDER BY SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END ELSE 0 END) DESC) AS user_rank,
    0 AS user_payin_count,
    0 AS user_payout_count,
    0.0 AS user_payin_volume_usd,
    0.0 AS user_payout_volume_usd
FROM tmp_merchant_analysis ma
JOIN tables t ON ma.merchant_id = t.`MERCHANT ID`
LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
WHERE DATE(t.`PAY TIME`) BETWEEN DATE_SUB(@analysis_date, INTERVAL 7 DAY) AND @analysis_date
  AND UPPER(t.`STATUS`) = 'SUCCESS'
GROUP BY ma.merchant_id, ma.merchant_name, t.`USER ID`;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_risk_assessment AS
SELECT 
    ma.*,
    CASE 
        WHEN ma.large_transaction_ratio_percent > 30 THEN 3
        WHEN ma.large_transaction_ratio_percent > 20 THEN 2
        WHEN ma.large_transaction_ratio_percent > 10 THEN 1
        ELSE 0
    END AS large_transaction_risk_score,
    CASE 
        WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.8 THEN 3
        WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.5 THEN 2
        WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.3 THEN 1
        ELSE 0
    END AS payin_payout_imbalance_score,
    CASE 
        WHEN ma.off_hours_count * 100.0 / NULLIF(ma.recent_7day_count, 0) > 30 THEN 2
        WHEN ma.off_hours_count * 100.0 / NULLIF(ma.recent_7day_count, 0) > 15 THEN 1
        ELSE 0
    END AS off_hours_activity_score,
    (
        CASE WHEN ma.large_transaction_ratio_percent > 30 THEN 3 ELSE 0 END +
        CASE WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.8 THEN 3 ELSE 0 END +
        CASE WHEN ma.off_hours_count * 100.0 / NULLIF(ma.recent_7day_count, 0) > 30 THEN 2 ELSE 0 END +
        CASE WHEN ma.risk_level = 'high' THEN 2 ELSE 0 END
    ) AS total_risk_score,
    CASE 
        WHEN (
            CASE WHEN ma.large_transaction_ratio_percent > 30 THEN 3 ELSE 0 END +
            CASE WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.8 THEN 3 ELSE 0 END +
            CASE WHEN ma.off_hours_count * 100.0 / NULLIF(ma.recent_7day_count, 0) > 30 THEN 2 ELSE 0 END +
            CASE WHEN ma.risk_level = 'high' THEN 2 ELSE 0 END
        ) >= 8 THEN 'HIGH_RISK'
        WHEN (
            CASE WHEN ma.large_transaction_ratio_percent > 30 THEN 3 ELSE 0 END +
            CASE WHEN ABS(ma.payin_volume_usd - ma.payout_volume_usd) / NULLIF(GREATEST(ma.payin_volume_usd, ma.payout_volume_usd), 0) > 0.8 THEN 3 ELSE 0 END +
            CASE WHEN ma.off_hours_count * 100.0 / NULLIF(ma.recent_7day_count, 0) > 30 THEN 2 ELSE 0 END +
            CASE WHEN ma.risk_level = 'high' THEN 2 ELSE 0 END
        ) >= 4 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_level_assessment
FROM tmp_merchant_analysis ma;


SELECT 
    'RANDOM_SAMPLE_SUMMARY' AS report_type,
    merchant_id AS "Merchant ID",
    merchant_name AS "Merchant Name",
    industry AS "Industry",
    risk_level AS "Original Risk Level",
    join_date AS "Join Date",
    recent_7day_count AS "7-Day Transaction Count",
    ROUND(recent_7day_volume_usd, 2) AS "7-Day Volume (USD)",
    ROUND(avg_transaction_value_usd, 2) AS "Avg Transaction Value (USD)",
    ROUND(median_transaction_value_usd, 2) AS "Median Transaction Value (USD)",
    
    -- 风险指标
    large_transaction_count AS "Large Transaction Count",
    ROUND(large_transaction_ratio_percent, 2) AS "Large Transaction Ratio (%)",
    
    -- Payin/Payout分析
    payin_count AS "Payin Count",
    payout_count AS "Payout Count",
    ROUND(payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(ABS(payin_volume_usd - payout_volume_usd) / NULLIF(GREATEST(payin_volume_usd, payout_volume_usd), 0) * 100, 2) AS "Payin/Payout Imbalance (%)",
    
    -- 渠道分析
    unique_channels AS "Unique Channels",
    
    -- 时间模式
    off_hours_count AS "Off-Hours Count",
    weekend_count AS "Weekend Count",
    ROUND(off_hours_count * 100.0 / NULLIF(recent_7day_count, 0), 2) AS "Off-Hours Ratio (%)",
    
    -- 风险评估
    total_risk_score AS "Total Risk Score",
    risk_level_assessment AS "Assessed Risk Level",
    
    CASE 
        WHEN risk_level_assessment = 'HIGH_RISK' THEN '🔴 高风险 - 需要立即深入调查'
        WHEN risk_level_assessment = 'MEDIUM_RISK' THEN '🟡 中风险 - 需要加强监控'
        WHEN risk_level_assessment = 'LOW_RISK' THEN '🟢 低风险 - 保持正常监控'
        ELSE '⚪ 风险等级错误'
    END AS "Risk Assessment Note"
    
FROM tmp_risk_assessment
ORDER BY total_risk_score DESC, recent_7day_volume_usd DESC;

SELECT 
    'TOP_USERS_ANALYSIS' AS report_type,
    tua.merchant_id AS "Merchant ID",
    tua.merchant_name AS "Merchant Name",
    tua.user_rank AS "User Rank",
    tua.user_id AS "User ID",
    tua.user_name AS "User Name",
    tua.risk_status AS "User Risk Status",
    tua.user_transaction_count AS "Transaction Count",
    ROUND(tua.user_volume_usd, 2) AS "Volume (USD)",
    
    -- Payin/Payout分析
    tua.user_payin_count AS "Payin Count",
    tua.user_payout_count AS "Payout Count",
    ROUND(tua.user_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(tua.user_payout_volume_usd, 2) AS "Payout Volume (USD)",
    
    -- 用户集中度
    ROUND(tua.user_volume_usd * 100.0 /
          NULLIF(SUM(tua.user_volume_usd) OVER (PARTITION BY tua.merchant_id), 0), 2)
    AS "User Share in Merchant (%)",
    
    CASE 
        WHEN tua.user_rank <= 5 THEN '🔴 Top 5用户'
        WHEN tua.user_rank <= 10 THEN '🟡 Top 6-10用户'
        WHEN tua.user_rank <= 20 THEN '🟢 Top 11-20用户'
        ELSE '⚪ 其他用户'
    END AS "User Importance Level",
    
    CASE 
        WHEN tua.risk_status = 'high' THEN '⚠️ 高风险用户'
        WHEN tua.risk_status = 'medium' THEN '⚡ 中风险用户'
        WHEN tua.risk_status = 'low' THEN '✅ 低风险用户'
        ELSE '❓ 风险状态未知'
    END AS "User Risk Alert"
    
FROM tmp_top_user_analysis tua
WHERE tua.user_rank <= 20  -- 只显示Top 20用户
ORDER BY tua.merchant_id, tua.user_rank;