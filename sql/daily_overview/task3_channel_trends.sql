-- SQL任务3：渠道趋势分析（7天）
-- 目标：观察Top商户在各渠道的7日变动，监控渠道集中度
USE test_database;
-- 设置查询参数
SET @analysis_date = CURRENT_DATE();
SET @start_date = DATE_SUB(@analysis_date, INTERVAL 7 DAY);

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_top_merchants AS
SELECT *
FROM (
    SELECT 
        t.`MERCHANT ID` AS merchant_id,
        MAX(t.`MERCHANT NAME`) AS merchant_name,
        SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
        ELSE 0 END) AS total_7day_volume_usd,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 1 END) AS total_7day_success_count,
        RANK() OVER(ORDER BY SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
        ELSE 0 END) DESC) AS merchant_rank
    FROM tables t
    LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
    WHERE DATE(t.`PAY TIME`) BETWEEN @start_date AND @analysis_date
    GROUP BY t.`MERCHANT ID`
) tm_all
WHERE merchant_rank <= 20;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_daily_channel_performance AS
SELECT 
    base.transaction_date,
    base.merchant_id,
    base.merchant_name,
    base.channel,
    base.daily_channel_volume_usd,
    base.daily_channel_count,
    base.daily_channel_unique_users,
    SUM(base.daily_channel_volume_usd) OVER(PARTITION BY base.transaction_date, base.merchant_id) AS merchant_daily_total_usd,
    base.daily_channel_volume_usd * 100.0 / NULLIF(SUM(base.daily_channel_volume_usd) OVER(PARTITION BY base.transaction_date, base.merchant_id), 0) AS channel_share_percent,
    RANK() OVER(PARTITION BY base.transaction_date, base.merchant_id ORDER BY base.daily_channel_volume_usd DESC) AS channel_rank
FROM (
    SELECT 
        DATE(t.`PAY TIME`) AS transaction_date,
        t.`MERCHANT ID` AS merchant_id,
        MAX(t.`MERCHANT NAME`) AS merchant_name,
        t.`PAY CHANNEL` AS channel,
        SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
        ELSE 0 END) AS daily_channel_volume_usd,
        COUNT(*) AS daily_channel_count,
        COUNT(DISTINCT t.`USER ID`) AS daily_channel_unique_users
    FROM tables t
    LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
    WHERE DATE(t.`PAY TIME`) BETWEEN @start_date AND @analysis_date
      AND t.`MERCHANT ID` IN (SELECT merchant_id FROM tmp_top_merchants)
    GROUP BY DATE(t.`PAY TIME`), t.`MERCHANT ID`, t.`PAY CHANNEL`
) base;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_channel_concentration AS
SELECT 
    merchant_id,
    merchant_name,
    channel,
    SUM(daily_channel_volume_usd) AS total_channel_volume_usd,
    SUM(daily_channel_count) AS total_channel_count,
    AVG(channel_share_percent) AS avg_channel_share_percent,
    SUM(POW(channel_share_percent, 2)) / 100.0 AS concentration_index,
    CASE WHEN AVG(channel_share_percent) > 10 THEN 1 ELSE 0 END AS major_channels_count,
    STDDEV(channel_share_percent) AS channel_share_volatility
FROM tmp_daily_channel_performance
GROUP BY merchant_id, merchant_name, channel;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_channel_anomaly_detection AS
SELECT 
    dcp.*,
    LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date) AS prev_day_share,
    CASE 
        WHEN LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date) > 0
        THEN (channel_share_percent - LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date)) * 100.0 /
             LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date)
        ELSE NULL
    END AS share_change_percent,
    CASE 
        WHEN channel_share_percent > 80 THEN 'HIGHLY_CONCENTRATED'
        WHEN channel_share_percent - LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date) > 30 THEN 'SUDDEN_INCREASE'
        WHEN LAG(channel_share_percent, 1) OVER(PARTITION BY merchant_id, channel ORDER BY transaction_date) - channel_share_percent > 30 THEN 'SUDDEN_DECREASE'
        WHEN channel_rank = 1 AND channel_share_percent < 30 THEN 'FRAGMENTED_LEADER'
        ELSE 'NORMAL'
    END AS anomaly_flag
FROM tmp_daily_channel_performance dcp;


-- 最终输出1：Top商户渠道分布概览
SELECT 
    'CHANNEL_DISTRIBUTION' AS report_type,
    tm.merchant_rank AS "Merchant Rank",
    tm.merchant_id AS "Merchant ID",
    tm.merchant_name AS "Merchant Name",
    
    cc.channel AS "Channel",
    ROUND(cc.total_channel_volume_usd, 2) AS "7-Day Volume (USD)",
    cc.total_channel_count AS "7-Day Count",
    ROUND(cc.avg_channel_share_percent, 2) AS "Avg Share (%)",
    cc.major_channels_count AS "Major Channels",
    ROUND(cc.concentration_index, 2) AS "Concentration Index",
    
    CASE 
        WHEN cc.concentration_index > 60 THEN 'HIGHLY_CONCENTRATED'
        WHEN cc.concentration_index > 40 THEN 'MODERATELY_CONCENTRATED'
        WHEN cc.major_channels_count = 1 THEN 'SINGLE_CHANNEL_DOMINANT'
        ELSE 'WELL_DISTRIBUTED'
    END AS "Distribution Pattern",
    
    CASE 
        WHEN cc.channel_share_volatility > 15 THEN 'HIGHLY_VOLATILE'
        WHEN cc.channel_share_volatility > 8 THEN 'MODERATELY_VOLATILE'
        ELSE 'STABLE'
    END AS "Stability Assessment"
    
FROM tmp_top_merchants tm
JOIN tmp_channel_concentration cc ON tm.merchant_id = cc.merchant_id
ORDER BY tm.merchant_rank, cc.total_channel_volume_usd DESC;

-- 最终输出2：渠道异常变动监控
SELECT 
    'CHANNEL_ANOMALY' AS report_type,
    ad.transaction_date AS "Date",
    ad.merchant_id AS "Merchant ID",
    ad.merchant_name AS "Merchant Name",
    ad.channel AS "Channel",
    ROUND(ad.daily_channel_volume_usd, 2) AS "Daily Volume (USD)",
    ad.daily_channel_count AS "Daily Count",
    ROUND(ad.channel_share_percent, 2) AS "Channel Share (%)",
    ad.channel_rank AS "Channel Rank",
    
    CASE 
        WHEN ad.anomaly_flag = 'HIGHLY_CONCENTRATED' THEN '⚠️ 渠道高度集中'
        WHEN ad.anomaly_flag = 'SUDDEN_INCREASE' THEN '🔺 渠道份额激增'
        WHEN ad.anomaly_flag = 'SUDDEN_DECREASE' THEN '🔻 渠道份额骤降'
        WHEN ad.anomaly_flag = 'FRAGMENTED_LEADER' THEN '⚡ 领导渠道分散'
        ELSE '✅ 正常'
    END AS "Anomaly Alert",
    
    CASE 
        WHEN ad.share_change_percent IS NOT NULL 
        THEN CONCAT(ROUND(ad.share_change_percent, 1), '%')
        ELSE 'N/A'
    END AS "Change from Prev Day",
    
    -- 风险评估
    CASE 
        WHEN ad.anomaly_flag IN ('SUDDEN_INCREASE', 'SUDDEN_DECREASE') AND ABS(ad.share_change_percent) > 50
        THEN 'HIGH_RISK'
        WHEN ad.anomaly_flag IN ('SUDDEN_INCREASE', 'SUDDEN_DECREASE') AND ABS(ad.share_change_percent) > 30
        THEN 'MEDIUM_RISK'
        WHEN ad.anomaly_flag = 'HIGHLY_CONCENTRATED'
        THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END AS "Risk Level"
    
FROM tmp_channel_anomaly_detection ad
WHERE ad.anomaly_flag != 'NORMAL'
   OR (ad.share_change_percent IS NOT NULL AND ABS(ad.share_change_percent) > 20)
ORDER BY ad.transaction_date DESC, ad.merchant_id, ad.channel_share_percent DESC;

-- 最终输出3：渠道集中度风险汇总
WITH risk_summary AS (
    SELECT 
        tm.merchant_id,
        tm.merchant_name,
        tm.merchant_rank,
        MAX(tm.total_7day_volume_usd) AS total_7day_volume_usd,
    
        -- 风险指标计算
        MAX(CASE WHEN cc.concentration_index > 60 THEN 1 ELSE 0 END) AS high_concentration_flag,
        MAX(CASE WHEN cc.major_channels_count = 1 THEN 1 ELSE 0 END) AS single_channel_flag,
        MAX(CASE WHEN cc.channel_share_volatility > 15 THEN 1 ELSE 0 END) AS high_volatility_flag,
        
        COUNT(CASE WHEN ad.anomaly_flag != 'NORMAL' THEN 1 END) AS anomaly_count,
        MAX(ad.channel_share_percent) AS max_channel_share_percent,
        
        -- 计算综合风险评分
        (
            MAX(CASE WHEN cc.concentration_index > 60 THEN 3 ELSE 0 END) +
            MAX(CASE WHEN cc.major_channels_count = 1 THEN 2 ELSE 0 END) +
            MAX(CASE WHEN cc.channel_share_volatility > 15 THEN 2 ELSE 0 END) +
            COUNT(CASE WHEN ad.anomaly_flag != 'NORMAL' THEN 1 END)
        ) AS risk_score
        
    FROM tmp_top_merchants tm
    JOIN tmp_channel_concentration cc ON tm.merchant_id = cc.merchant_id
    LEFT JOIN tmp_channel_anomaly_detection ad ON tm.merchant_id = ad.merchant_id
    GROUP BY tm.merchant_id, tm.merchant_name, tm.merchant_rank
)
SELECT 
    'RISK_SUMMARY' AS report_type,
    merchant_rank AS "Merchant Rank",
    merchant_id AS "Merchant ID",
    merchant_name AS "Merchant Name",
    
    
    CASE 
        WHEN risk_score >= 6 THEN '🔴 HIGH_RISK'
        WHEN risk_score >= 4 THEN '🟡 MEDIUM_RISK'
        WHEN risk_score >= 2 THEN '🟢 LOW_RISK'
        ELSE '✅ NORMAL'
    END AS "Overall Risk Level",
    
    risk_score AS "Risk Score",
    anomaly_count AS "Anomaly Count",
    ROUND(max_channel_share_percent, 1) AS "Max Channel Share (%)",
    
    CONCAT(
        CASE WHEN high_concentration_flag = 1 THEN '高集中度 ' ELSE '' END,
        CASE WHEN single_channel_flag = 1 THEN '单一渠道依赖 ' ELSE '' END,
        CASE WHEN high_volatility_flag = 1 THEN '渠道波动大' ELSE '' END
    ) AS "Risk Factors",
    
    CASE 
        WHEN risk_score >= 6 THEN '建议立即检查渠道策略和风险控制'
        WHEN risk_score >= 4 THEN '建议加强监控和定期评估'
        WHEN risk_score >= 2 THEN '保持正常监控频率'
        ELSE '无特殊风险，正常监控'
    END AS "Recommended Action"
    
FROM risk_summary
ORDER BY risk_score DESC, total_7day_volume_usd DESC, merchant_id;