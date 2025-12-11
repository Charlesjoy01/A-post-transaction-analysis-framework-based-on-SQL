-- SQL任务6：特定用户分析（告警触发）
-- 目标：分析被认定为可疑的用户，识别洗钱和异常行为模式

-- 设置查询参数（这些参数在实际使用时需要传入）
USE test_database;
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET collation_connection = 'utf8mb4_0900_ai_ci';
SET @target_user_id = 'CHL__RUT__205443584';
SET @analysis_date = CURRENT_DATE();
SET @lookback_days = 180;
SET @start_date = DATE_SUB(@analysis_date, INTERVAL @lookback_days DAY);

DROP TEMPORARY TABLE IF EXISTS user_basics;
CREATE TEMPORARY TABLE user_basics AS
SELECT 
    u.user_id,
    u.user_name,
    NULL AS email,
    NULL AS phone,
    NULL AS tax_id,
    u.registration_date,
    u.risk_status,
    NULL AS user_type,
    NULL AS country,
    NULL AS verification_level,
    DATEDIFF(@analysis_date, u.registration_date) AS user_age_days,
    CASE WHEN DATEDIFF(@analysis_date, u.registration_date) < 30 THEN 'NEW_USER' ELSE 'EXISTING_USER' END AS user_category,
    CASE 
        WHEN u.risk_status = 'high' THEN 'HIGH_RISK'
        WHEN u.risk_status = 'medium' THEN 'MEDIUM_RISK'
        WHEN u.risk_status = 'low' THEN 'LOW_RISK'
        ELSE 'UNKNOWN_RISK'
    END AS risk_classification
FROM users u
WHERE u.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id;

DROP TEMPORARY TABLE IF EXISTS daily_user_activity;
CREATE TEMPORARY TABLE daily_user_activity AS
SELECT 
    t.transaction_date,
    COUNT(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS daily_success_count,
    COUNT(*) AS daily_total_count,
    SUM(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
    AVG(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd END) AS daily_avg_amount_usd,
    MAX(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd END) AS daily_max_amount_usd,
    MIN(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd END) AS daily_min_amount_usd,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS daily_payin_count,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) AS daily_payout_count,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS daily_payin_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS daily_payout_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN t.amount_usd ELSE 0 END) AS daily_net_flow_usd,
    IFNULL(COUNT(CASE WHEN (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 0) AS daily_success_rate,
    COUNT(DISTINCT t.channel) AS daily_unique_channels,
    COUNT(DISTINCT t.merchant_id) AS daily_unique_merchants,
    COUNT(CASE WHEN HOUR(t.transaction_time) BETWEEN 0 AND 6 THEN 1 END) AS off_hours_count,
    COUNT(CASE WHEN DAYOFWEEK(CAST(t.transaction_date AS DATE)) IN (1, 7) THEN 1 END) AS weekend_count,
    COUNT(CASE WHEN HOUR(t.transaction_time) BETWEEN 9 AND 17 THEN 1 END) AS business_hours_count
FROM transactions t
WHERE t.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
GROUP BY t.transaction_date;
SET @avg_daily_amount_usd = (
  SELECT AVG(daily_avg_amount_usd)
  FROM daily_user_activity
  WHERE daily_avg_amount_usd > 0
);

DROP TEMPORARY TABLE IF EXISTS daily_user_activity_summary;
CREATE TEMPORARY TABLE daily_user_activity_summary AS
SELECT
  COUNT(*) AS active_days_count,
  SUM(daily_volume_usd) AS total_volume_usd,
  SUM(daily_success_count) AS total_success_count,
  AVG(daily_avg_amount_usd) AS avg_daily_amount_usd,
  MAX(daily_max_amount_usd) AS max_single_transaction_usd,
  SUM(off_hours_count) AS total_off_hours_transactions,
  SUM(weekend_count) AS total_weekend_transactions,
  SUM(daily_payin_volume_usd) AS total_payin_volume_usd,
  SUM(daily_payout_volume_usd) AS total_payout_volume_usd
FROM daily_user_activity;

DROP TEMPORARY TABLE IF EXISTS large_transaction_analysis;
CREATE TEMPORARY TABLE large_transaction_analysis AS
SELECT 
    t.transaction_date,
    t.transaction_time,
    t.merchant_id,
    m.merchant_name,
    m.industry,
    m.risk_level AS merchant_risk_level,
    t.amount_usd,
    t.transaction_type,
    t.channel,
    t.status,
    CASE 
        WHEN t.amount_usd > COALESCE(@avg_daily_amount_usd, 0) * 5 THEN 'VERY_LARGE_TRANSACTION'
        WHEN t.amount_usd > COALESCE(@avg_daily_amount_usd, 0) * 3 THEN 'LARGE_TRANSACTION'
        ELSE 'NORMAL_TRANSACTION'
    END AS large_transaction_flag,
    CASE 
        WHEN t.amount_usd BETWEEN 4990 AND 5010 THEN 'SENSITIVE_4999'
        WHEN t.amount_usd BETWEEN 9990 AND 10010 THEN 'SENSITIVE_9999'
        WHEN t.amount_usd BETWEEN 9900 AND 10100 THEN 'ROUND_NUMBER_10K'
        WHEN t.amount_usd BETWEEN 4900 AND 5100 THEN 'ROUND_NUMBER_5K'
        WHEN MOD(ROUND(t.amount_usd), 1000) = 0 THEN 'ROUND_THOUSAND'
        WHEN MOD(ROUND(t.amount_usd), 100) = 0 THEN 'ROUND_HUNDRED'
        ELSE 'NORMAL_AMOUNT'
    END AS amount_pattern_flag,
    (
      SELECT COUNT(*)
      FROM transactions t2
      WHERE t2.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
        AND t2.transaction_date = t.transaction_date
        AND (LOWER(t2.transaction_type) COLLATE utf8mb4_0900_ai_ci) = (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci)
        AND (UPPER(t2.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
        AND t2.transaction_time BETWEEN t.transaction_time - INTERVAL 1 HOUR AND t.transaction_time
    ) AS same_day_same_type_count,
    COUNT(*) OVER(PARTITION BY t.merchant_id, t.transaction_date) AS daily_merchant_count,
    TIMESTAMPDIFF(MINUTE, 
                  LAG(t.transaction_time) OVER(PARTITION BY t.transaction_date ORDER BY t.transaction_time), 
                  t.transaction_time) AS time_interval_minutes
FROM transactions t
JOIN merchants m ON (t.merchant_id COLLATE utf8mb4_0900_ai_ci) = (m.merchant_id COLLATE utf8mb4_0900_ai_ci)
WHERE t.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
  AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID');
-- 外部风险与大额/敏感金额汇总在各自临时表中一次性计算，避免重开临时表

DROP TEMPORARY TABLE IF EXISTS merchant_concentration;
CREATE TEMPORARY TABLE merchant_concentration AS
SELECT 
    t.merchant_id,
    m.merchant_name,
    m.industry,
    m.risk_level AS merchant_risk_level,
    COUNT(*) AS total_transaction_count,
    SUM(t.amount_usd) AS total_volume_usd,
    AVG(t.amount_usd) AS avg_amount_usd,
    MAX(t.amount_usd) AS max_amount_usd,
    MIN(t.amount_usd) AS min_amount_usd,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN 1 END) AS payin_count,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN 1 END) AS payout_count,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN t.amount_usd ELSE 0 END) AS payin_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN t.amount_usd ELSE 0 END) AS payout_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN t.amount_usd ELSE 0 END) AS net_flow_usd,
    COUNT(DISTINCT t.transaction_date) AS active_days,
    MIN(t.transaction_date) AS first_transaction_date,
    MAX(t.transaction_date) AS last_transaction_date,
    RANK() OVER(ORDER BY SUM(t.amount_usd) DESC) AS merchant_rank,
    SUM(t.amount_usd) * 100.0 / NULLIF((
        SELECT SUM(amount_usd)
        FROM transactions
        WHERE user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
          AND (UPPER(status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
          AND transaction_date BETWEEN @start_date AND @analysis_date
    ), 0) AS volume_share_percent
FROM transactions t
JOIN merchants m ON (t.merchant_id COLLATE utf8mb4_0900_ai_ci) = (m.merchant_id COLLATE utf8mb4_0900_ai_ci)
WHERE t.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
  AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level;

DROP TEMPORARY TABLE IF EXISTS lta_summary;
CREATE TEMPORARY TABLE lta_summary AS
SELECT 
  COUNT(CASE WHEN large_transaction_flag IN ('LARGE_TRANSACTION','VERY_LARGE_TRANSACTION') THEN 1 END) AS large_transaction_count,
  COUNT(CASE WHEN amount_pattern_flag IN ('SENSITIVE_4999','SENSITIVE_9999') THEN 1 END) AS sensitive_amount_count
FROM large_transaction_analysis;

DROP TEMPORARY TABLE IF EXISTS mc_summary;
CREATE TEMPORARY TABLE mc_summary AS
SELECT 
  SUM(CASE WHEN merchant_rank <= 3 THEN volume_share_percent ELSE 0 END) AS top3_merchant_concentration,
  COUNT(CASE WHEN UPPER(CONVERT(merchant_risk_level USING utf8mb4)) = 'HIGH' THEN 1 END) AS high_risk_merchant_count
FROM merchant_concentration;

DROP TEMPORARY TABLE IF EXISTS user_external_risk_summary;
CREATE TEMPORARY TABLE user_external_risk_summary AS
SELECT
  COALESCE(mc.top3_merchant_concentration, 0) AS top3_merchant_concentration,
  COALESCE(mc.high_risk_merchant_count, 0) AS high_risk_merchant_count,
  COALESCE(lta.large_transaction_count, 0) AS large_transaction_count,
  COALESCE(lta.sensitive_amount_count, 0) AS sensitive_amount_count
FROM mc_summary mc
CROSS JOIN lta_summary lta;

DROP TEMPORARY TABLE IF EXISTS monthly_user_trends;
CREATE TEMPORARY TABLE monthly_user_trends AS
SELECT 
    DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
    YEAR(t.transaction_date) AS year_num,
    MONTH(t.transaction_date) AS month_num,
    COUNT(*) AS monthly_transaction_count,
    SUM(t.amount_usd) AS monthly_volume_usd,
    AVG(t.amount_usd) AS monthly_avg_amount_usd,
    MAX(t.amount_usd) AS monthly_max_amount_usd,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN 1 END) AS monthly_payin_count,
    COUNT(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN 1 END) AS monthly_payout_count,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN t.amount_usd ELSE 0 END) AS monthly_payin_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN t.amount_usd ELSE 0 END) AS monthly_payout_volume_usd,
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payin' THEN t.amount_usd ELSE 0 END) - 
    SUM(CASE WHEN (LOWER(t.transaction_type) COLLATE utf8mb4_0900_ai_ci) = 'payout' THEN t.amount_usd ELSE 0 END) AS monthly_net_flow_usd,
    COUNT(DISTINCT t.merchant_id) AS monthly_unique_merchants,
    COUNT(CASE WHEN t.amount_usd > 5000 THEN 1 END) AS monthly_large_transactions,
    COUNT(CASE WHEN t.amount_usd BETWEEN 4990 AND 5010 THEN 1 END) AS monthly_sensitive_4999,
    COUNT(CASE WHEN t.amount_usd BETWEEN 9990 AND 10010 THEN 1 END) AS monthly_sensitive_9999
FROM transactions t
WHERE t.user_id COLLATE utf8mb4_0900_ai_ci = @target_user_id
  AND t.transaction_date BETWEEN @start_date AND @analysis_date
  AND (UPPER(t.status) COLLATE utf8mb4_0900_ai_ci) IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
GROUP BY DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date);

DROP TEMPORARY TABLE IF EXISTS risk_scoring;
CREATE TEMPORARY TABLE risk_scoring AS
SELECT 
    ub.user_id,
    ub.risk_classification,
    s.active_days_count,
    s.total_volume_usd,
    s.total_success_count,
    s.avg_daily_amount_usd,
    s.max_single_transaction_usd,
    e.top3_merchant_concentration,
    e.high_risk_merchant_count,
    e.large_transaction_count,
    e.sensitive_amount_count,
    s.total_off_hours_transactions,
    s.total_weekend_transactions,
    s.total_payin_volume_usd,
    s.total_payout_volume_usd,
    (
        CASE WHEN e.top3_merchant_concentration > 70 THEN 4 ELSE 0 END +
        CASE WHEN e.high_risk_merchant_count > 2 THEN 3 ELSE 0 END +
        CASE WHEN e.large_transaction_count > 5 THEN 3 ELSE 0 END +
        CASE WHEN e.sensitive_amount_count > 3 THEN 2 ELSE 0 END +
        CASE WHEN s.total_payin_volume_usd > s.total_payout_volume_usd * 3 THEN 2 ELSE 0 END +
        CASE WHEN s.total_off_hours_transactions > s.total_success_count * 0.3 THEN 2 ELSE 0 END +
        CASE WHEN s.max_single_transaction_usd > COALESCE(@avg_daily_amount_usd, 0) * 10 THEN 2 ELSE 0 END
    ) AS total_risk_score
FROM user_basics ub
CROSS JOIN daily_user_activity_summary s
CROSS JOIN user_external_risk_summary e;

-- 最终输出1：用户基础信息和总体表现
SELECT 
    'USER_BASIC_INFO' AS report_section,
    ub.user_id AS "User ID",
    ub.user_name AS "User Name",
    ub.email AS "Email",
    ub.phone AS "Phone",
    ub.tax_id AS "Tax ID",
    ub.country AS "Country",
    ub.user_type AS "User Type",
    ub.verification_level AS "Verification Level",
    ub.registration_date AS "Registration Date",
    ub.user_age_days AS "User Age (Days)",
    ub.user_category AS "User Category",
    ub.risk_status AS "Original Risk Status",
    ub.risk_classification AS "Risk Classification",
    
    s.total_success_count AS "Total Success Count",
    s.total_volume_usd AS "Total Volume (USD)",
    s.active_days_count AS "Active Days Count",
    s.avg_daily_amount_usd AS "Avg Transaction Amount (USD)",
    s.max_single_transaction_usd AS "Max Single Transaction (USD)",
    s.total_payin_volume_usd AS "Total Payin Volume (USD)",
    s.total_payout_volume_usd AS "Total Payout Volume (USD)",
    s.total_payin_volume_usd - s.total_payout_volume_usd AS "Net Flow (USD)"
    
FROM user_basics ub
CROSS JOIN daily_user_activity_summary s;

-- 最终输出2：最近30天日级别活动分析
SELECT 
    'RECENT_DAILY_ACTIVITY' AS report_section,
    dua.transaction_date AS "Date",
    dua.daily_success_count AS "Success Count",
    dua.daily_total_count AS "Total Count",
    ROUND(dua.daily_volume_usd, 2) AS "Volume (USD)",
    ROUND(dua.daily_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(dua.daily_max_amount_usd, 2) AS "Max Amount (USD)",
    ROUND(dua.daily_success_rate, 2) AS "Success Rate (%)",
    
    -- Payin/Payout
    dua.daily_payin_count AS "Payin Count",
    dua.daily_payout_count AS "Payout Count",
    ROUND(dua.daily_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(dua.daily_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(dua.daily_net_flow_usd, 2) AS "Net Flow (USD)",
    
    -- 活动模式
    dua.daily_unique_channels AS "Unique Channels",
    dua.daily_unique_merchants AS "Unique Merchants",
    dua.off_hours_count AS "Off-Hours Count",
    dua.weekend_count AS "Weekend Count",
    dua.business_hours_count AS "Business Hours Count",
    
    -- 时间模式分析
    CASE 
        WHEN dua.off_hours_count > dua.business_hours_count THEN '非工作时间为主'
        WHEN dua.off_hours_count > 0 THEN '存在非工作时间活动'
        ELSE '正常工作时间'
    END AS "Activity Pattern",
    
    CASE 
        WHEN dua.weekend_count > 0 THEN '周末有活动'
        ELSE '周末无活动'
    END AS "Weekend Activity"
    
FROM daily_user_activity dua
WHERE dua.transaction_date >= DATE_SUB(@analysis_date, INTERVAL 30 DAY)
ORDER BY dua.transaction_date DESC;

-- 最终输出3：大额交易和敏感金额分析
SELECT 
    'LARGE_TRANSACTION_ANALYSIS' AS report_section,
    lta.transaction_date AS "Date",
    lta.transaction_time AS "Time",
    lta.merchant_id AS "Merchant ID",
    lta.merchant_name AS "Merchant Name",
    lta.industry AS "Industry",
    lta.merchant_risk_level AS "Merchant Risk Level",
    ROUND(lta.amount_usd, 2) AS "Amount (USD)",
    lta.transaction_type AS "Type",
    lta.channel AS "Channel",
    lta.large_transaction_flag AS "Size Flag",
    lta.amount_pattern_flag AS "Pattern Flag",
    lta.same_day_same_type_count AS "Same Day Same Type Count",
    lta.daily_merchant_count AS "Daily Merchant Count",
    lta.time_interval_minutes AS "Time Interval (Min)",
    
    -- 风险评估
    CASE 
        WHEN lta.large_transaction_flag = 'VERY_LARGE_TRANSACTION' THEN '超大额交易'
        WHEN lta.large_transaction_flag = 'LARGE_TRANSACTION' THEN '大额交易'
        ELSE '🟢 正常交易'
    END AS "Size Risk",
    
    CASE 
        WHEN lta.amount_pattern_flag IN ('SENSITIVE_4999', 'SENSITIVE_9999') THEN '敏感金额'
        WHEN lta.amount_pattern_flag IN ('ROUND_NUMBER_10K', 'ROUND_NUMBER_5K') THEN '整数金额'
        WHEN lta.amount_pattern_flag IN ('ROUND_THOUSAND', 'ROUND_HUNDRED') THEN '规整金额'
        ELSE '✅ 正常金额'
    END AS "Pattern Risk",
    
    CASE 
        WHEN UPPER(CONVERT(lta.merchant_risk_level USING utf8mb4)) = 'HIGH' THEN '高风险商户'
        WHEN UPPER(CONVERT(lta.merchant_risk_level USING utf8mb4)) = 'MEDIUM' THEN '中风险商户'
        WHEN UPPER(CONVERT(lta.merchant_risk_level USING utf8mb4)) = 'LOW' THEN '低风险商户'
        ELSE '风险等级未知'
    END AS "Merchant Risk Alert"
    
FROM large_transaction_analysis lta
WHERE lta.large_transaction_flag != 'NORMAL_TRANSACTION'
   OR lta.amount_pattern_flag != 'NORMAL_AMOUNT'
ORDER BY lta.transaction_date DESC, lta.amount_usd DESC;

-- 最终输出4：商户集中度分析
SELECT 
    'MERCHANT_CONCENTRATION_ANALYSIS' AS report_section,
    mc.merchant_rank AS "Merchant Rank",
    mc.merchant_id AS "Merchant ID",
    mc.merchant_name AS "Merchant Name",
    mc.industry AS "Industry",
    mc.merchant_risk_level AS "Merchant Risk Level",
    mc.total_transaction_count AS "Transaction Count",
    ROUND(mc.total_volume_usd, 2) AS "Total Volume (USD)",
    ROUND(mc.avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(mc.max_amount_usd, 2) AS "Max Amount (USD)",
    ROUND(mc.volume_share_percent, 2) AS "Volume Share (%)",
    mc.active_days AS "Active Days",
    mc.first_transaction_date AS "First Transaction",
    mc.last_transaction_date AS "Last Transaction",
    
    -- Payin/Payout分析
    mc.payin_count AS "Payin Count",
    mc.payout_count AS "Payout Count",
    ROUND(mc.payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(mc.payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(mc.net_flow_usd, 2) AS "Net Flow (USD)",
    
    -- 集中度风险
    CASE 
        WHEN mc.volume_share_percent > 50 THEN '高度集中'
        WHEN mc.volume_share_percent > 30 THEN '中度集中'
        WHEN mc.volume_share_percent > 15 THEN '轻度集中'
        ELSE '⚪ 分散'
    END AS "Concentration Level",
    
    CASE 
        WHEN UPPER(CONVERT(mc.merchant_risk_level USING utf8mb4)) = 'HIGH' THEN '高风险商户'
        WHEN UPPER(CONVERT(mc.merchant_risk_level USING utf8mb4)) = 'MEDIUM' THEN '中风险商户'
        WHEN UPPER(CONVERT(mc.merchant_risk_level USING utf8mb4)) = 'LOW' THEN '低风险商户'
        ELSE '风险等级未知'
    END AS "Merchant Risk Alert",
    
    CASE 
        WHEN mc.net_flow_usd > 0 THEN '净流入'
        WHEN mc.net_flow_usd < 0 THEN '净流出'
        ELSE '平衡'
    END AS "Flow Direction"
    
FROM merchant_concentration mc
WHERE mc.merchant_rank <= 10  -- 只显示Top 10商户
ORDER BY mc.merchant_rank;

-- 最终输出5：月度趋势分析（前6个月）
SELECT 
    'MONTHLY_TRENDS_ANALYSIS' AS report_section,
    mut.month_year AS "Month-Year",
    mut.monthly_transaction_count AS "Transaction Count",
    ROUND(mut.monthly_volume_usd, 2) AS "Volume (USD)",
    ROUND(mut.monthly_avg_amount_usd, 2) AS "Avg Amount (USD)",
    ROUND(mut.monthly_max_amount_usd, 2) AS "Max Amount (USD)",
    
    -- Payin/Payout
    mut.monthly_payin_count AS "Payin Count",
    mut.monthly_payout_count AS "Payout Count",
    ROUND(mut.monthly_payin_volume_usd, 2) AS "Payin Volume (USD)",
    ROUND(mut.monthly_payout_volume_usd, 2) AS "Payout Volume (USD)",
    ROUND(mut.monthly_net_flow_usd, 2) AS "Net Flow (USD)",
    
    -- 商户集中度
    mut.monthly_unique_merchants AS "Unique Merchants",
    
    -- 大额交易
    mut.monthly_large_transactions AS "Large Transactions",
    mut.monthly_sensitive_4999 AS "Sensitive 4999",
    mut.monthly_sensitive_9999 AS "Sensitive 9999",
    
    -- 环比分析
    ROUND(
        (mut.monthly_volume_usd - LAG(mut.monthly_volume_usd) OVER(ORDER BY mut.year_num, mut.month_num)) * 100.0 / 
        NULLIF(LAG(mut.monthly_volume_usd) OVER(ORDER BY mut.year_num, mut.month_num), 0), 2
    ) AS "Volume MoM Change (%)",
    
    ROUND(
        (mut.monthly_transaction_count - LAG(mut.monthly_transaction_count) OVER(ORDER BY mut.year_num, mut.month_num)) * 100.0 / 
        NULLIF(LAG(mut.monthly_transaction_count) OVER(ORDER BY mut.year_num, mut.month_num), 0), 2
    ) AS "Count MoM Change (%)",
    
    -- 流量方向
    CASE 
        WHEN mut.monthly_net_flow_usd > 0 THEN '净流入'
        WHEN mut.monthly_net_flow_usd < 0 THEN '净流出'
        ELSE '平衡'
    END AS "Monthly Flow Direction"
    
FROM monthly_user_trends mut
ORDER BY mut.year_num DESC, mut.month_num DESC;

-- 最终输出6：综合风险评估和建议
SELECT 
    'COMPREHENSIVE_RISK_ASSESSMENT' AS report_section,
    
    -- 基础风险指标
    rs.total_risk_score AS "Total Risk Score (0-16)",
    rs.risk_classification AS "Original Risk Classification",
    
    -- 具体风险因素
    rs.large_transaction_count AS "Large Transaction Count",
    rs.sensitive_amount_count AS "Sensitive Amount Count",
    rs.top3_merchant_concentration AS "Top3 Merchant Concentration (%)",
    rs.high_risk_merchant_count AS "High Risk Merchant Count",
    rs.total_off_hours_transactions AS "Off-Hours Transactions",
    
    -- 流量分析
    ROUND(rs.total_payin_volume_usd, 2) AS "Total Payin Volume (USD)",
    ROUND(rs.total_payout_volume_usd, 2) AS "Total Payout Volume (USD)",
    ROUND(rs.total_payin_volume_usd - rs.total_payout_volume_usd, 2) AS "Net Flow (USD)",
    
    -- 综合风险评级
    CASE 
        WHEN rs.total_risk_score >= 12 THEN 'CRITICAL RISK - 需要立即冻结账户并启动调查'
        WHEN rs.total_risk_score >= 8 THEN 'HIGH RISK - 需要立即加强监控和深度调查'
        WHEN rs.total_risk_score >= 4 THEN 'MEDIUM RISK - 需要增加监控频率和定期评估'
        WHEN rs.total_risk_score >= 2 THEN 'LOW RISK - 保持正常监控但需关注变化'
        ELSE 'MINIMAL RISK - 正常监控'
    END AS "Overall Risk Assessment",
    
    -- 主要风险因素
    CASE 
        WHEN rs.total_risk_score >= 8 THEN 
            CONCAT(
                '主要风险因素：',
                CASE WHEN rs.large_transaction_count > 5 THEN '大额交易频繁；' ELSE '' END,
                CASE WHEN rs.sensitive_amount_count > 3 THEN '敏感金额模式；' ELSE '' END,
                CASE WHEN rs.top3_merchant_concentration > 80 THEN '商户高度集中；' ELSE '' END,
                CASE WHEN rs.high_risk_merchant_count > 2 THEN '高风险商户交易；' ELSE '' END,
                CASE WHEN rs.total_off_hours_transactions > rs.total_success_count * 0.3 THEN '非工作时间活动异常；' ELSE '' END,
                CASE WHEN rs.total_payin_volume_usd > rs.total_payout_volume_usd * 3 THEN '资金沉淀异常；' ELSE '' END
            )
        ELSE '风险水平在可接受范围内'
    END AS "Key Risk Factors",
    
    -- 建议措施
    CASE 
        WHEN rs.total_risk_score >= 12 THEN 
            '建议立即措施：1)冻结账户防止进一步风险；2)启动紧急调查程序；3)联系用户核实身份和交易目的；4)向监管机构报告可疑活动；5)保存所有相关证据'
        WHEN rs.total_risk_score >= 8 THEN 
            '建议措施：1)立即加强交易监控；2)要求用户提供资金来源证明；3)限制大额交易；4)进行增强尽职调查；5)考虑账户限制措施'
        WHEN rs.total_risk_score >= 4 THEN 
            '建议措施：1)增加监控频率；2)定期要求用户更新信息；3)分析交易模式变化；4)建立风险预警机制；5)准备应急处理方案'
        WHEN rs.total_risk_score >= 2 THEN 
            '建议措施：1)保持正常监控；2)关注风险指标变化；3)定期回顾用户风险状况；4)教育用户合规交易'
        ELSE 
            '建议：保持标准监控程序，定期例行检查'
    END AS "Recommended Actions"
    
FROM risk_scoring rs;
