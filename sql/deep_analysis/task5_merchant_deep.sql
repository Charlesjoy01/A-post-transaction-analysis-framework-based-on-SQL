-- SQL任务5：特定商户深度分析（Weekly或抽检触发）
-- 目标：深入分析单一商户的交易行为，识别可疑模式
USE test_database;
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET collation_connection = 'utf8mb4_unicode_ci';
-- 设置查询参数（这些参数在实际使用时需要传入）
SET @target_merchant_id = '1743996732237458';  -- 需要分析的特定商户ID
SET @analysis_date = CURRENT_DATE();
SET @lookback_days = 180;  -- 分析180天的历史数据
SET @start_date = DATE_SUB(@analysis_date, INTERVAL @lookback_days DAY);

DROP TEMPORARY TABLE IF EXISTS tmp_merchant_basics;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_merchant_basics AS
SELECT 
    m.merchant_id,
    m.merchant_name,
    m.industry,
    m.risk_level,
    m.join_date,
    m.business_type,
    m.registered_country,
    DATEDIFF(@analysis_date, m.join_date) AS merchant_age_days,
    COUNT(CASE WHEN t.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY) AND t.status = 'success' THEN 1 END) AS recent_30day_count,
    SUM(CASE WHEN t.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY) AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS recent_30day_volume_usd
FROM merchants m
LEFT JOIN transactions t ON m.merchant_id COLLATE utf8mb4_unicode_ci = t.merchant_id COLLATE utf8mb4_unicode_ci
WHERE m.merchant_id COLLATE utf8mb4_unicode_ci = @target_merchant_id COLLATE utf8mb4_unicode_ci
GROUP BY m.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date, m.business_type, m.registered_country;

DROP TEMPORARY TABLE IF EXISTS tmp_daily_trends;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_daily_trends AS
SELECT 
    t.transaction_date,
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
    COUNT(*) AS daily_total_count,
    SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
    AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS daily_avg_amount_usd,
    COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS daily_payin_count,
    COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS daily_payout_count,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_payin_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_payout_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_net_flow_usd,
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0 / COUNT(*) AS daily_success_rate
FROM transactions t
WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = @target_merchant_id COLLATE utf8mb4_unicode_ci
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
GROUP BY t.transaction_date;

DROP TEMPORARY TABLE IF EXISTS tmp_moving_averages;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_moving_averages AS
SELECT 
    dt.*,
    AVG(daily_volume_usd) OVER(ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30day_volume_usd,
    AVG(daily_success_count) OVER(ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30day_count,
    AVG(daily_volume_usd) OVER(ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7day_volume_usd,
    AVG(daily_success_count) OVER(ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7day_count,
    CASE 
        WHEN AVG(daily_volume_usd) OVER(ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0
        THEN (daily_volume_usd - AVG(daily_volume_usd) OVER(ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 100.0 /
             AVG(daily_volume_usd) OVER(ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        ELSE 0
    END AS volume_volatility_percent
FROM tmp_daily_trends dt;

DROP TEMPORARY TABLE IF EXISTS tmp_anomaly_detection;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_anomaly_detection AS
SELECT 
    ma.*,
    CASE 
        WHEN ABS(volume_volatility_percent) > 15 THEN 'VOLUME_ANOMALY'
        WHEN daily_success_rate < 90 THEN 'SUCCESS_RATE_ANOMALY'
        WHEN ABS(daily_net_flow_usd) > ma_30day_volume_usd * 2 THEN 'NET_FLOW_ANOMALY'
        ELSE 'NORMAL'
    END AS anomaly_type,
    CASE 
        WHEN daily_volume_usd > ma_30day_volume_usd * 1.15 THEN 'SUDDEN_INCREASE'
        WHEN daily_volume_usd < ma_30day_volume_usd * 0.85 THEN 'SUDDEN_DECREASE'
        WHEN daily_volume_usd > ma_7day_volume_usd AND ma_7day_volume_usd > ma_30day_volume_usd THEN 'UPWARD_TREND'
        WHEN daily_volume_usd < ma_7day_volume_usd AND ma_7day_volume_usd < ma_30day_volume_usd THEN 'DOWNWARD_TREND'
        ELSE 'STABLE'
    END AS trend_direction,
    CASE 
        WHEN ABS(volume_volatility_percent) > 25 THEN 'HIGH_RISK'
        WHEN ABS(volume_volatility_percent) > 15 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS volatility_risk_level
FROM tmp_moving_averages ma;

DROP TEMPORARY TABLE IF EXISTS tmp_top_user_analysis;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_top_user_analysis AS
SELECT 
    t.user_id,
    u.user_name,
    u.risk_status,
    u.registration_date,
    COUNT(CASE WHEN t.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY) AND t.status = 'success' THEN 1 END) AS user_30day_count,
    SUM(CASE WHEN t.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY) AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_30day_volume_usd,
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS user_total_count,
    SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_total_volume_usd,
    AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS user_avg_amount_usd,
    MAX(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS user_max_amount_usd,
    COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS user_payin_count,
    COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS user_payout_count,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_payin_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_payout_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_net_flow_usd,
    RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS user_rank
FROM transactions t
JOIN users u ON t.user_id COLLATE utf8mb4_unicode_ci = u.user_id COLLATE utf8mb4_unicode_ci
WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = @target_merchant_id COLLATE utf8mb4_unicode_ci
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
GROUP BY t.user_id, u.user_name, u.risk_status, u.registration_date;

DROP TEMPORARY TABLE IF EXISTS tmp_monthly_trends;
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_monthly_trends AS
SELECT 
    DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
    YEAR(t.transaction_date) AS year_num,
    MONTH(t.transaction_date) AS month_num,
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_success_count,
    COUNT(*) AS monthly_total_count,
    SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_volume_usd,
    AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_avg_amount_usd,
    COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS monthly_payin_count,
    COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS monthly_payout_count,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_payin_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_payout_volume_usd,
    SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_net_flow_usd,
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0 / COUNT(*) AS monthly_success_rate
FROM transactions t
WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = @target_merchant_id COLLATE utf8mb4_unicode_ci
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
GROUP BY DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
ORDER BY year_num DESC, month_num DESC;

-- 最终输出1：商户基础信息和总体表现
SELECT 
    'MERCHANT_BASIC_INFO' AS report_section,
    mb.merchant_id AS "Merchant ID",
    mb.merchant_name AS "Merchant Name",
    mb.industry AS "Industry",
    mb.business_type AS "Business Type",
    mb.risk_level AS "Original Risk Level",
    mb.registered_country AS "Registered Country",
    mb.merchant_age_days AS "Merchant Age (Days)",
    mb.join_date AS "Join Date",
    
    -- 最近30天表现
    mb.recent_30day_count AS "Recent 30-Day Count",
    ROUND(mb.recent_30day_volume_usd, 2) AS "Recent 30-Day Volume (USD)",
    
    -- 180天总计
    agg_dt.total_active_days AS "Total Active Days",
    agg_dt.total_success_count AS "Total Success Count",
    ROUND(agg_dt.total_volume_usd, 2) AS "Total Volume (USD)",
    ROUND(agg_dt.avg_amount_usd, 2) AS "Avg Transaction Amount (USD)",
    
    -- 异常统计
    agg_anom.anomaly_days_count AS "Anomaly Days Count",
    agg_anom.high_risk_days_count AS "High Risk Days Count"
    
FROM tmp_merchant_basics mb
CROSS JOIN (
    SELECT 
        COUNT(*) AS total_active_days,
        SUM(daily_success_count) AS total_success_count,
        SUM(daily_volume_usd) AS total_volume_usd,
        AVG(daily_avg_amount_usd) AS avg_amount_usd
    FROM tmp_daily_trends
) agg_dt
CROSS JOIN (
    SELECT 
        COUNT(CASE WHEN anomaly_type != 'NORMAL' THEN 1 END) AS anomaly_days_count,
        COUNT(CASE WHEN volatility_risk_level = 'HIGH_RISK' THEN 1 END) AS high_risk_days_count
    FROM tmp_anomaly_detection
) agg_anom;

-- 最终输出2：最近30天日级别趋势
SELECT 
    'RECENT_DAILY_TRENDS' AS report_section,
    ad.transaction_date AS "Date",
    ad.daily_success_count AS "Success Count",
    ad.daily_total_count AS "Total Count",
    ROUND(ad.daily_volume_usd, 2) AS "Volume (USD)",
    ROUND(ad.daily_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(ad.daily_success_rate, 2) AS "Success Rate (%)",
    
    -- Payin/Payout
    ad.daily_payin_count AS "Payin Count",
    ad.daily_payout_count AS "Payout Count",
    ROUND(ad.daily_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(ad.daily_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(ad.daily_net_flow_usd, 2) AS "Net Flow (USD)",
    
    -- 趋势分析
    ROUND(ad.ma_7day_volume_usd, 2) AS "7-Day MA Volume (USD)",
    ROUND(ad.ma_30day_volume_usd, 2) AS "30-Day MA Volume (USD)",
    ROUND(ad.volume_volatility_percent, 2) AS "Volume Volatility (%)",
    ad.trend_direction AS "Trend Direction",
    ad.volatility_risk_level AS "Risk Level",
    ad.anomaly_type AS "Anomaly Type"
    
FROM tmp_anomaly_detection ad
WHERE ad.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY)
ORDER BY ad.transaction_date DESC;

-- 最终输出3：用户集中度分析
SELECT 
    'USER_CONCENTRATION_ANALYSIS' AS report_section,
    tua.user_rank AS "User Rank",
    tua.user_id AS "User ID",
    tua.user_name AS "User Name",
    tua.risk_status AS "User Risk Status",
    DATEDIFF(@analysis_date, tua.registration_date) AS "User Age (Days)",
    
    -- 交易统计
    tua.user_total_count AS "Total Transaction Count",
    tua.user_30day_count AS "30-Day Count",
    ROUND(tua.user_total_volume_usd, 2) AS "Total Volume (USD)",
    ROUND(tua.user_30day_volume_usd, 2) AS "30-Day Volume (USD)",
    ROUND(tua.user_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(tua.user_max_amount_usd, 2) AS "Max Amount (USD)",
    
    -- 集中度
    ROUND(tua.user_total_volume_usd * 100.0 / NULLIF(SUM(tua.user_total_volume_usd) OVER (), 0), 2) AS "Volume Share (%)",
    ROUND(tua.user_total_count * 100.0 / NULLIF(SUM(tua.user_total_count) OVER (), 0), 2) AS "Count Share (%)",
    
    -- Payin/Payout
    tua.user_payin_count AS "Payin Count",
    tua.user_payout_count AS "Payout Count",
    ROUND(tua.user_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(tua.user_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(tua.user_net_flow_usd, 2) AS "Net Flow (USD)",
    
    CASE 
        WHEN tua.user_rank <= 5 THEN '🔴 TOP 5 - 高度关注'
        WHEN tua.user_rank <= 10 THEN '🟡 TOP 6-10 - 中等关注'
        WHEN tua.user_rank <= 20 THEN '🟢 TOP 11-20 - 一般关注'
        ELSE '⚪ 其他用户'
    END AS "Concentration Level"
    
FROM tmp_top_user_analysis tua
WHERE tua.user_rank <= 20
ORDER BY tua.user_rank;

-- 最终输出4：月度趋势分析（前6个月）
SELECT 
    'MONTHLY_TRENDS' AS report_section,
    mt.month_year AS "Month-Year",
    mt.monthly_success_count AS "Success Count",
    mt.monthly_total_count AS "Total Count",
    ROUND(mt.monthly_volume_usd, 2) AS "Volume (USD)",
    ROUND(mt.monthly_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(mt.monthly_success_rate, 2) AS "Success Rate (%)",
    
    -- Payin/Payout
    mt.monthly_payin_count AS "Payin Count",
    mt.monthly_payout_count AS "Payout Count",
    ROUND(mt.monthly_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(mt.monthly_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(mt.monthly_net_flow_usd, 2) AS "Net Flow (USD)",
    
    -- 环比分析（与上月对比）
    ROUND(
        (mt.monthly_volume_usd - LAG(mt.monthly_volume_usd) OVER(ORDER BY mt.year_num, mt.month_num)) * 100.0 / 
        NULLIF(LAG(mt.monthly_volume_usd) OVER(ORDER BY mt.year_num, mt.month_num), 0), 2
    ) AS "Volume MoM Change (%)",
    
    ROUND(
        (mt.monthly_success_count - LAG(mt.monthly_success_count) OVER(ORDER BY mt.year_num, mt.month_num)) * 100.0 / 
        NULLIF(LAG(mt.monthly_success_count) OVER(ORDER BY mt.year_num, mt.month_num), 0), 2
    ) AS "Count MoM Change (%)",
    
    CASE 
        WHEN mt.monthly_net_flow_usd > 0 THEN '🔺 净流入'
        WHEN mt.monthly_net_flow_usd < 0 THEN '🔻 净流出'
        ELSE '➖ 平衡'
    END AS "Flow Direction"
    
FROM tmp_monthly_trends mt
ORDER BY mt.year_num DESC, mt.month_num DESC;

-- 最终输出5：综合风险评估
SELECT 
    'COMPREHENSIVE_RISK_ASSESSMENT' AS report_section,
    CASE 
        WHEN ad.cnt20 > 10 THEN 'HIGH'
        WHEN ad.cnt15 > 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Volatility Risk",
    CASE 
        WHEN tua.vol_top5 > tua.vol_total * 0.8 THEN 'HIGH'
        WHEN tua.vol_top10 > tua.vol_total * 0.9 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Concentration Risk",
    CASE 
        WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.7 THEN 'HIGH'
        WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Payin/Payout Imbalance Risk",
    CASE 
        WHEN (
            (CASE WHEN ad.cnt20 > 10 THEN 3 ELSE 0 END) +
            (CASE WHEN tua.vol_top5 > tua.vol_total * 0.8 THEN 3 ELSE 0 END) +
            (CASE WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.7 THEN 3 ELSE 0 END)
        ) >= 6 THEN '🔴 HIGH RISK - 需要立即关注和深度调查'
        WHEN (
            (CASE WHEN ad.cnt15 > 5 THEN 2 ELSE 0 END) +
            (CASE WHEN tua.vol_top10 > tua.vol_total * 0.9 THEN 2 ELSE 0 END) +
            (CASE WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.5 THEN 2 ELSE 0 END)
        ) >= 2 THEN '🟡 MEDIUM RISK - 需要加强监控'
        ELSE '🟢 LOW RISK - 保持正常监控'
    END AS "Overall Risk Assessment",
    CASE 
        WHEN (
            (CASE WHEN ad.cnt20 > 10 THEN 3 ELSE 0 END) +
            (CASE WHEN tua.vol_top5 > tua.vol_total * 0.8 THEN 3 ELSE 0 END) +
            (CASE WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.7 THEN 3 ELSE 0 END)
        ) >= 6 THEN '建议：1)立即暂停可疑交易 2)联系商户核实业务情况 3)进行实地尽职调查 4)考虑风险管控措施'
        WHEN (
            (CASE WHEN ad.cnt15 > 5 THEN 2 ELSE 0 END) +
            (CASE WHEN tua.vol_top10 > tua.vol_total * 0.9 THEN 2 ELSE 0 END) +
            (CASE WHEN ABS(dvol.payin_sum - dvol.payout_sum) / NULLIF(GREATEST(dvol.payin_sum, dvol.payout_sum), 0) > 0.5 THEN 2 ELSE 0 END)
        ) >= 2 THEN '建议：1)增加监控频率 2)要求商户提供业务说明 3)分析用户背景 4)定期评估风险状况'
        ELSE '建议：保持现有监控频率，定期回顾风险状况'
    END AS "Recommended Actions"
FROM 
    (SELECT 
        SUM(CASE WHEN ABS(volume_volatility_percent) > 20 THEN 1 ELSE 0 END) AS cnt20,
        SUM(CASE WHEN ABS(volume_volatility_percent) > 15 THEN 1 ELSE 0 END) AS cnt15
     FROM tmp_anomaly_detection) ad
CROSS JOIN
    (SELECT 
        SUM(CASE WHEN user_rank <= 5 THEN user_total_volume_usd ELSE 0 END) AS vol_top5,
        SUM(CASE WHEN user_rank <= 10 THEN user_total_volume_usd ELSE 0 END) AS vol_top10,
        SUM(user_total_volume_usd) AS vol_total
     FROM tmp_top_user_analysis) tua
CROSS JOIN
    (SELECT 
        SUM(daily_payin_volume_usd) AS payin_sum,
        SUM(daily_payout_volume_usd) AS payout_sum
    FROM tmp_daily_trends) dvol;

CREATE OR REPLACE VIEW v_merchant_deep_basic_info AS
SELECT * FROM v_merchant_deep_basic_info_all;

CREATE OR REPLACE VIEW v_merchant_deep_recent_daily_trends AS
SELECT * FROM v_merchant_deep_recent_daily_trends_all;

CREATE OR REPLACE VIEW v_merchant_deep_top_users AS
SELECT * FROM v_merchant_deep_top_users_all;

CREATE OR REPLACE VIEW v_merchant_deep_monthly_trends AS
SELECT * FROM v_merchant_deep_monthly_trends_all;

CREATE OR REPLACE VIEW v_merchant_deep_risk_summary AS
SELECT * FROM v_merchant_deep_risk_summary_all;
-- 无参数通用视图：按商户维度，查询时用 WHERE 过滤 merchant_id

CREATE OR REPLACE VIEW v_merchant_deep_basic_info_all AS
SELECT 
    'MERCHANT_BASIC_INFO' AS report_section,
    m.merchant_id AS "Merchant ID",
    m.merchant_name AS "Merchant Name",
    m.industry AS "Industry",
    m.business_type AS "Business Type",
    m.risk_level AS "Original Risk Level",
    m.registered_country AS "Registered Country",
    DATEDIFF(CURRENT_DATE(), m.join_date) AS "Merchant Age (Days)",
    m.join_date AS "Join Date",
    (SELECT COUNT(*)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
       AND t.status = 'success') AS "Recent 30-Day Count",
    (SELECT ROUND(SUM(t.amount_usd), 2)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
       AND t.status = 'success') AS "Recent 30-Day Volume (USD)",
    (SELECT COUNT(DISTINCT t.transaction_date)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE) AS "Total Active Days",
    (SELECT COUNT(CASE WHEN t.status = 'success' THEN 1 END)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE) AS "Total Success Count",
    (SELECT ROUND(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END), 2)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE) AS "Total Volume (USD)",
    (SELECT ROUND(AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END), 2)
     FROM transactions t
     WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
       AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE) AS "Avg Transaction Amount (USD)",
    (SELECT COUNT(*)
     FROM (
        SELECT t.transaction_date,
               COUNT(*) AS total_cnt,
               COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS success_cnt,
               SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payin_usd,
               SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payout_usd
        FROM transactions t
        WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
          AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
        GROUP BY t.transaction_date
     ) d
     WHERE (success_cnt * 100.0 / NULLIF(total_cnt, 0)) < 90
        OR ABS(payin_usd - payout_usd) > (payin_usd + payout_usd)) AS "Anomaly Days Count",
    (SELECT COUNT(*)
     FROM (
        SELECT t.transaction_date,
               SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payin_usd,
               SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payout_usd,
               SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS total_usd
        FROM transactions t
        WHERE t.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
          AND t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
        GROUP BY t.transaction_date
     ) d2
     WHERE ABS(payin_usd - payout_usd) > 2 * total_usd) AS "High Risk Days Count"
FROM merchants m;

CREATE OR REPLACE VIEW v_merchant_deep_recent_daily_trends_all AS
SELECT 
    'RECENT_DAILY_TRENDS' AS report_section,
    t.merchant_id AS "Merchant ID",
    t.transaction_date AS "Date",
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS "Success Count",
    COUNT(*) AS "Total Count",
    ROUND(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Volume (USD)",
    ROUND(AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END), 2) AS "Avg Amount (USD)",
    ROUND(COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0 / COUNT(*), 2) AS "Success Rate (%)",
    COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS "Payin Count",
    COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS "Payout Count",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Payin Volume (USD)",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Payout Volume (USD)",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) - 
          SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Net Flow (USD)"
FROM transactions t
WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE
GROUP BY t.merchant_id, t.transaction_date;

CREATE OR REPLACE VIEW v_merchant_deep_top_users_all AS
SELECT 
    'USER_CONCENTRATION_ANALYSIS' AS report_section,
    s.merchant_id AS "Merchant ID",
    s.user_rank AS "User Rank",
    s.user_id AS "User ID",
    s.user_name AS "User Name",
    s.risk_status AS "User Risk Status",
    DATEDIFF(CURRENT_DATE(), s.registration_date) AS "User Age (Days)",
    s.user_total_count AS "Total Transaction Count",
    s.user_30day_count AS "30-Day Count",
    ROUND(s.user_total_volume_usd, 2) AS "Total Volume (USD)",
    ROUND(s.user_30day_volume_usd, 2) AS "30-Day Volume (USD)",
    ROUND(s.user_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(s.user_max_amount_usd, 2) AS "Max Amount (USD)",
    s.user_payin_count AS "Payin Count",
    s.user_payout_count AS "Payout Count",
    ROUND(s.user_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(s.user_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(s.user_net_flow_usd, 2) AS "Net Flow (USD)",
    ROUND(s.user_total_volume_usd * 100.0 / NULLIF(SUM(s.user_total_volume_usd) OVER (PARTITION BY s.merchant_id), 0), 2) AS "Volume Share (%)",
    ROUND(s.user_total_count * 100.0 / NULLIF(SUM(s.user_total_count) OVER (PARTITION BY s.merchant_id), 0), 2) AS "Count Share (%)"
FROM (
    SELECT 
        t.merchant_id,
        t.user_id,
        u.user_name,
        u.risk_status,
        u.registration_date,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS user_total_count,
        COUNT(CASE WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND t.status = 'success' THEN 1 END) AS user_30day_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_total_volume_usd,
        SUM(CASE WHEN t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_30day_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS user_avg_amount_usd,
        MAX(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS user_max_amount_usd,
        COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS user_payin_count,
        COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS user_payout_count,
        SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_payin_volume_usd,
        SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_payout_volume_usd,
        SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) - 
        SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_net_flow_usd,
        RANK() OVER(PARTITION BY t.merchant_id ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS user_rank
    FROM transactions t
    JOIN users u ON u.user_id COLLATE utf8mb4_unicode_ci = t.user_id COLLATE utf8mb4_unicode_ci
    WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
    GROUP BY t.merchant_id, t.user_id, u.user_name, u.risk_status, u.registration_date
) s
WHERE s.user_rank <= 20;

CREATE OR REPLACE VIEW v_merchant_deep_monthly_trends_all AS
SELECT 
    'MONTHLY_TRENDS' AS report_section,
    t.merchant_id AS "Merchant ID",
    DATE_FORMAT(t.transaction_date, '%Y-%m') AS "Month-Year",
    COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS "Success Count",
    COUNT(*) AS "Total Count",
    ROUND(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Volume (USD)",
    ROUND(AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END), 2) AS "Avg Amount (USD)",
    ROUND(COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0 / COUNT(*), 2) AS "Success Rate (%)",
    COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN 1 END) AS "Payin Count",
    COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN 1 END) AS "Payout Count",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Payin Volume (USD)",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Payout Volume (USD)",
    ROUND(SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) -
          SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END), 2) AS "Net Flow (USD)"
FROM transactions t
WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
GROUP BY t.merchant_id, DATE_FORMAT(t.transaction_date, '%Y-%m');

CREATE OR REPLACE VIEW v_merchant_deep_risk_summary_all AS
SELECT 
    'COMPREHENSIVE_RISK_ASSESSMENT' AS report_section,
    m.merchant_id AS "Merchant ID",
    CASE 
        WHEN vol.high_days > 10 THEN 'HIGH'
        WHEN vol.medium_days > 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Volatility Risk",
    CASE 
        WHEN conc.top5_volume > conc.total_volume * 0.8 THEN 'HIGH'
        WHEN conc.top10_volume > conc.total_volume * 0.9 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Concentration Risk",
    CASE 
        WHEN ABS(flow.payin_sum - flow.payout_sum) / NULLIF(GREATEST(flow.payin_sum, flow.payout_sum), 0) > 0.7 THEN 'HIGH'
        WHEN ABS(flow.payin_sum - flow.payout_sum) / NULLIF(GREATEST(flow.payin_sum, flow.payout_sum), 0) > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS "Payin/Payout Imbalance Risk",
    CASE 
        WHEN ((CASE WHEN vol.high_days > 10 THEN 3 ELSE 0 END) +
              (CASE WHEN conc.top5_volume > conc.total_volume * 0.8 THEN 3 ELSE 0 END) +
              (CASE WHEN ABS(flow.payin_sum - flow.payout_sum) / NULLIF(GREATEST(flow.payin_sum, flow.payout_sum), 0) > 0.7 THEN 3 ELSE 0 END)) >= 6
        THEN '🔴 HIGH RISK - 需要立即关注和深度调查'
        WHEN ((CASE WHEN vol.medium_days > 5 THEN 2 ELSE 0 END) +
              (CASE WHEN conc.top10_volume > conc.total_volume * 0.9 THEN 2 ELSE 0 END) +
              (CASE WHEN ABS(flow.payin_sum - flow.payout_sum) / NULLIF(GREATEST(flow.payin_sum, flow.payout_sum), 0) > 0.5 THEN 2 ELSE 0 END)) >= 2
        THEN '🟡 MEDIUM RISK - 需要加强监控'
        ELSE '🟢 LOW RISK - 保持正常监控'
    END AS "Overall Risk Assessment"
FROM merchants m
JOIN (
    SELECT v.merchant_id,
           SUM(CASE WHEN ABS(v.volatility_percent) > 15 THEN 1 ELSE 0 END) AS medium_days,
           SUM(CASE WHEN ABS(v.volatility_percent) > 25 THEN 1 ELSE 0 END) AS high_days
    FROM (
        SELECT d.merchant_id,
               d.transaction_date,
               CASE 
                   WHEN AVG(d.daily_volume_usd) OVER(PARTITION BY d.merchant_id ORDER BY d.transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0
                   THEN (d.daily_volume_usd - AVG(d.daily_volume_usd) OVER(PARTITION BY d.merchant_id ORDER BY d.transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) * 100.0 /
                        AVG(d.daily_volume_usd) OVER(PARTITION BY d.merchant_id ORDER BY d.transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
                   ELSE 0
               END AS volatility_percent
        FROM (
            SELECT t.merchant_id,
                   t.transaction_date,
                   SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd
            FROM transactions t
            WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
            GROUP BY t.merchant_id, t.transaction_date
        ) d
    ) v
    GROUP BY v.merchant_id
) vol ON vol.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
JOIN (
    SELECT merchant_id,
           SUM(CASE WHEN user_rank <= 5 THEN user_total_volume_usd ELSE 0 END) AS top5_volume,
           SUM(CASE WHEN user_rank <= 10 THEN user_total_volume_usd ELSE 0 END) AS top10_volume,
           SUM(user_total_volume_usd) AS total_volume
    FROM (
        SELECT t.merchant_id,
               t.user_id,
               SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS user_total_volume_usd,
               RANK() OVER(PARTITION BY t.merchant_id ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS user_rank
        FROM transactions t
        WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
        GROUP BY t.merchant_id, t.user_id
    ) s
    GROUP BY merchant_id
) conc ON conc.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci
JOIN (
    SELECT t.merchant_id,
           SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payin_sum,
           SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' THEN t.amount_usd ELSE 0 END) AS payout_sum
    FROM transactions t
    WHERE t.transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE
    GROUP BY t.merchant_id
) flow ON flow.merchant_id COLLATE utf8mb4_unicode_ci = m.merchant_id COLLATE utf8mb4_unicode_ci;
