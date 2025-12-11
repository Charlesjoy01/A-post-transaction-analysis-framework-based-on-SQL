-- SQL任务8：渠道月度趋势分析
-- 目标：每月固定日期观测渠道变动，作为渠道健康度的一个指标

-- 设置查询参数
SET @analysis_date = '2025-11-25';
SET @current_month_start = DATE_FORMAT(@analysis_date, '%Y-%m-01');
SET @current_month_end = LAST_DAY(@analysis_date);
SET @lookback_months = 3;  -- 分析前3个月的历史数据

-- 最终输出1：Top商户渠道分布趋势
WITH 
current_month_top_merchants AS (
    SELECT * FROM (
        SELECT 
            t.merchant_id,
            m.merchant_name,
            m.industry,
            m.risk_level,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
            RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
          AND t.status = 'success'
        GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level
    ) s
    WHERE s.current_month_volume_rank <= 20
),
channel_monthly_analysis AS (
    SELECT 
        cmtm.merchant_id,
        cmtm.merchant_name,
        cmtm.industry,
        cmtm.risk_level,
        cmtm.current_month_volume_rank,
        t.channel,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_channel_success_count,
        COUNT(*) AS monthly_channel_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_channel_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_channel_avg_amount_usd,
        RANK() OVER(PARTITION BY t.merchant_id, DATE_FORMAT(t.transaction_date, '%Y-%m') 
                    ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_channel_rank
    FROM current_month_top_merchants cmtm
    JOIN transactions t ON cmtm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY cmtm.merchant_id, cmtm.merchant_name, cmtm.industry, cmtm.risk_level, cmtm.current_month_volume_rank,
             t.channel, DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_base AS (
    SELECT 
        cma.*,
        cma.monthly_channel_volume_usd * 100.0 /
            SUM(cma.monthly_channel_volume_usd) OVER(PARTITION BY cma.merchant_id, cma.month_year) AS monthly_channel_share_percent
    FROM channel_monthly_analysis cma
),
trend_calculation AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.industry,
        tb.risk_level,
        tb.current_month_volume_rank,
        tb.channel,
        tb.month_year,
        tb.year_num,
        tb.month_num,
        tb.monthly_channel_volume_usd,
        tb.monthly_channel_success_count,
        tb.monthly_channel_avg_amount_usd,
        tb.monthly_channel_rank,
        tb.monthly_channel_share_percent,
        LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_volume_usd,
        LAG(tb.monthly_channel_success_count, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_success_count,
        LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_share_percent,
        CASE 
            WHEN LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) > 0
            THEN (tb.monthly_channel_volume_usd - LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num)) * 100.0 /
                 LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num)
            ELSE NULL
        END AS mom_channel_volume_growth_percent,
        CASE 
            WHEN LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) > 0
            THEN (tb.monthly_channel_share_percent - LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num))
            ELSE NULL
        END AS mom_channel_share_change_percent,
        AVG(tb.monthly_channel_volume_usd) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3month_channel_volume_usd,
        AVG(tb.monthly_channel_share_percent) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3month_channel_share_percent
    FROM trend_base tb
),
channel_concentration_analysis AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.industry,
        tb.risk_level,
        tb.month_year,
        tb.year_num,
        tb.month_num,
        SUM(POW(tb.monthly_channel_share_percent, 2)) / 100.0 AS monthly_channel_concentration_index,
        COUNT(CASE WHEN tb.monthly_channel_share_percent > 10 THEN 1 END) AS major_channels_count,
        COUNT(CASE WHEN tb.monthly_channel_share_percent > 50 THEN 1 END) AS dominant_channels_count,
        -SUM(CASE WHEN tb.monthly_channel_share_percent > 0 THEN tb.monthly_channel_share_percent * LN(tb.monthly_channel_share_percent / 100.0) ELSE 0 END) AS channel_diversity_index,
        MAX(tb.monthly_channel_share_percent) AS max_channel_share_percent,
        GROUP_CONCAT(CASE WHEN tb.monthly_channel_rank = 1 THEN tb.channel END ORDER BY tb.monthly_channel_share_percent DESC SEPARATOR ', ') AS top_channel_names
    FROM trend_base tb
    GROUP BY tb.merchant_id, tb.merchant_name, tb.industry, tb.risk_level, tb.month_year, tb.year_num, tb.month_num
),
channel_risk_detection AS (
    SELECT 
        tc.merchant_id,
        tc.merchant_name,
        tc.industry,
        tc.risk_level,
        tc.current_month_volume_rank,
        tc.channel,
        tc.month_year,
        tc.monthly_channel_volume_usd,
        tc.monthly_channel_success_count,
        tc.monthly_channel_share_percent,
        tc.monthly_channel_rank,
        tc.mom_channel_volume_growth_percent,
        tc.mom_channel_share_change_percent,
        tc.ma_3month_channel_volume_usd,
        tc.ma_3month_channel_share_percent,
        CASE 
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 200 THEN 'EXTREME_VOLATILITY'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 100 THEN 'HIGH_VOLATILITY'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 50 THEN 'MODERATE_VOLATILITY'
            WHEN ABS(tc.mom_channel_share_change_percent) > 30 THEN 'SHARE_SHIFT_ANOMALY'
            WHEN tc.monthly_channel_share_percent > 80 THEN 'HIGH_CONCENTRATION'
            WHEN tc.monthly_channel_share_percent < 5 AND tc.ma_3month_channel_share_percent > 20 THEN 'SHARP_DECLINE'
            ELSE 'NORMAL'
        END AS channel_anomaly_flag,
        CASE 
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 200 OR tc.monthly_channel_share_percent > 90 THEN 'CRITICAL_RISK'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 100 OR ABS(tc.mom_channel_share_change_percent) > 40 THEN 'HIGH_RISK'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 50 OR ABS(tc.mom_channel_share_change_percent) > 20 THEN 'MEDIUM_RISK'
            WHEN tc.monthly_channel_share_percent > 70 OR tc.monthly_channel_share_percent < 10 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS channel_risk_level
    FROM trend_calculation tc
    WHERE tc.mom_channel_volume_growth_percent IS NOT NULL
)
SELECT 
    'TOP_MERCHANT_CHANNEL_TRENDS' AS report_type,
    crd.merchant_id AS `Merchant ID`,
    crd.merchant_name AS `Merchant Name`,
    crd.industry AS `Industry`,
    crd.risk_level AS `Original Risk Level`,
    crd.current_month_volume_rank AS `Current Month Volume Rank`,
    crd.channel AS `Channel`,
    crd.month_year AS `Month-Year`,
    ROUND(crd.monthly_channel_volume_usd, 2) AS `Channel Volume (USD)`,
    crd.monthly_channel_success_count AS `Channel Success Count`,
    ROUND(crd.monthly_channel_share_percent, 2) AS `Channel Share (%)`,
    crd.monthly_channel_rank AS `Channel Rank`,
    CASE 
        WHEN crd.mom_channel_volume_growth_percent > 100 THEN '🔺 爆发式增长'
        WHEN crd.mom_channel_volume_growth_percent > 50 THEN '🔼 快速增长'
        WHEN crd.mom_channel_volume_growth_percent > 20 THEN '⬆️ 稳步增长'
        WHEN crd.mom_channel_volume_growth_percent BETWEEN -20 AND 20 THEN '➖ 基本稳定'
        WHEN crd.mom_channel_volume_growth_percent > -50 THEN '⬇️ 明显下降'
        ELSE '🔻 急剧下降'
    END AS `Growth Trend`,
    ROUND(crd.mom_channel_volume_growth_percent, 2) AS `MoM Volume Growth (%)`,
    ROUND(crd.mom_channel_share_change_percent, 2) AS `MoM Share Change (%)`,
    ROUND(crd.ma_3month_channel_volume_usd, 2) AS `3-Month MA Volume (USD)`,
    ROUND(crd.ma_3month_channel_share_percent, 2) AS `3-Month MA Share (%)`,
    crd.channel_anomaly_flag AS `Anomaly Flag`,
    crd.channel_risk_level AS `Risk Level`,
    CASE 
        WHEN crd.channel_risk_level = 'CRITICAL_RISK' THEN '🔴 需要立即关注'
        WHEN crd.channel_risk_level = 'HIGH_RISK' THEN '🔴 需要加强监控'
        WHEN crd.channel_risk_level = 'MEDIUM_RISK' THEN '🟡 需要密切关注'
        WHEN crd.channel_risk_level = 'LOW_MEDIUM_RISK' THEN '🟢 保持正常监控'
        ELSE '✅ 正常范围'
    END AS `Risk Assessment Note`
FROM channel_risk_detection crd
WHERE crd.merchant_id IN (SELECT merchant_id FROM current_month_top_merchants)
ORDER BY crd.current_month_volume_rank, crd.monthly_channel_volume_usd DESC, crd.month_year DESC;

-- 最终输出2：渠道异常变动监控
WITH 
current_month_top_merchants AS (
    SELECT * FROM (
        SELECT 
            t.merchant_id,
            m.merchant_name,
            m.industry,
            m.risk_level,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
            RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
          AND t.status = 'success'
        GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level
    ) s
    WHERE s.current_month_volume_rank <= 20
),
channel_monthly_analysis AS (
    SELECT 
        cmtm.merchant_id,
        cmtm.merchant_name,
        cmtm.industry,
        cmtm.risk_level,
        cmtm.current_month_volume_rank,
        t.channel,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_channel_success_count,
        COUNT(*) AS monthly_channel_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_channel_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_channel_avg_amount_usd,
        RANK() OVER(PARTITION BY t.merchant_id, DATE_FORMAT(t.transaction_date, '%Y-%m') 
                    ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_channel_rank
    FROM current_month_top_merchants cmtm
    JOIN transactions t ON cmtm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY cmtm.merchant_id, cmtm.merchant_name, cmtm.industry, cmtm.risk_level, cmtm.current_month_volume_rank,
             t.channel, DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_base AS (
    SELECT 
        cma.*,
        cma.monthly_channel_volume_usd * 100.0 /
            SUM(cma.monthly_channel_volume_usd) OVER(PARTITION BY cma.merchant_id, cma.month_year) AS monthly_channel_share_percent
    FROM channel_monthly_analysis cma
),
trend_calculation AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.industry,
        tb.risk_level,
        tb.current_month_volume_rank,
        tb.channel,
        tb.month_year,
        tb.year_num,
        tb.month_num,
        tb.monthly_channel_volume_usd,
        tb.monthly_channel_success_count,
        tb.monthly_channel_avg_amount_usd,
        tb.monthly_channel_rank,
        tb.monthly_channel_share_percent,
        LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_volume_usd,
        LAG(tb.monthly_channel_success_count, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_success_count,
        LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) AS prev_month_share_percent,
        CASE 
            WHEN LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) > 0
            THEN (tb.monthly_channel_volume_usd - LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num)) * 100.0 /
                 LAG(tb.monthly_channel_volume_usd, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num)
            ELSE NULL
        END AS mom_channel_volume_growth_percent,
        CASE 
            WHEN LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num) > 0
            THEN (tb.monthly_channel_share_percent - LAG(tb.monthly_channel_share_percent, 1) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num))
            ELSE NULL
        END AS mom_channel_share_change_percent,
        AVG(tb.monthly_channel_volume_usd) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3month_channel_volume_usd,
        AVG(tb.monthly_channel_share_percent) OVER(PARTITION BY tb.merchant_id, tb.channel ORDER BY tb.year_num, tb.month_num ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3month_channel_share_percent
    FROM trend_base tb
),
channel_risk_detection AS (
    SELECT 
        tc.merchant_id,
        tc.merchant_name,
        tc.industry,
        tc.risk_level,
        tc.current_month_volume_rank,
        tc.channel,
        tc.month_year,
        tc.monthly_channel_volume_usd,
        tc.monthly_channel_success_count,
        tc.monthly_channel_share_percent,
        tc.monthly_channel_rank,
        tc.mom_channel_volume_growth_percent,
        tc.mom_channel_share_change_percent,
        tc.ma_3month_channel_volume_usd,
        tc.ma_3month_channel_share_percent,
        CASE 
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 200 THEN 'EXTREME_VOLATILITY'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 100 THEN 'HIGH_VOLATILITY'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 50 THEN 'MODERATE_VOLATILITY'
            WHEN ABS(tc.mom_channel_share_change_percent) > 30 THEN 'SHARE_SHIFT_ANOMALY'
            WHEN tc.monthly_channel_share_percent > 80 THEN 'HIGH_CONCENTRATION'
            WHEN tc.monthly_channel_share_percent < 5 AND tc.ma_3month_channel_share_percent > 20 THEN 'SHARP_DECLINE'
            ELSE 'NORMAL'
        END AS channel_anomaly_flag,
        CASE 
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 200 OR tc.monthly_channel_share_percent > 90 THEN 'CRITICAL_RISK'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 100 OR ABS(tc.mom_channel_share_change_percent) > 40 THEN 'HIGH_RISK'
            WHEN ABS(tc.mom_channel_volume_growth_percent) > 50 OR ABS(tc.mom_channel_share_change_percent) > 20 THEN 'MEDIUM_RISK'
            WHEN tc.monthly_channel_share_percent > 70 OR tc.monthly_channel_share_percent < 10 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS channel_risk_level
    FROM trend_calculation tc
    WHERE tc.mom_channel_volume_growth_percent IS NOT NULL
)
SELECT 
    'CHANNEL_ANOMALY_MONITORING' AS report_type,
    crd.merchant_id AS `Merchant ID`,
    crd.merchant_name AS `Merchant Name`,
    crd.industry AS `Industry`,
    crd.channel AS `Channel`,
    crd.month_year AS `Month-Year`,
    ROUND(crd.monthly_channel_volume_usd, 2) AS `Channel Volume (USD)`,
    ROUND(crd.monthly_channel_share_percent, 2) AS `Channel Share (%)`,
    ROUND(crd.mom_channel_volume_growth_percent, 2) AS `MoM Volume Growth (%)`,
    ROUND(crd.mom_channel_share_change_percent, 2) AS `MoM Share Change (%)`,
    crd.channel_anomaly_flag AS `Anomaly Type`,
    crd.channel_risk_level AS `Risk Level`,
    CASE 
        WHEN crd.channel_anomaly_flag = 'EXTREME_VOLATILITY' THEN '🔴 极端波动 - 立即调查'
        WHEN crd.channel_anomaly_flag = 'HIGH_VOLATILITY' THEN '🔴 高度波动 - 需要关注'
        WHEN crd.channel_anomaly_flag = 'MODERATE_VOLATILITY' THEN '🟡 中度波动 - 密切关注'
        WHEN crd.channel_anomaly_flag = 'SHARE_SHIFT_ANOMALY' THEN '⚡ 份额转移异常 - 分析原因'
        WHEN crd.channel_anomaly_flag = 'HIGH_CONCENTRATION' THEN '⚠️ 高度集中 - 风险提醒'
        WHEN crd.channel_anomaly_flag = 'SHARP_DECLINE' THEN '🔻 急剧下降 - 了解原因'
        ELSE '✅ 正常'
    END AS `Anomaly Assessment`,
    CASE 
        WHEN crd.channel_anomaly_flag IN ('EXTREME_VOLATILITY', 'HIGH_VOLATILITY') THEN 
            '建议：1)立即联系商户了解渠道变化原因；2)分析是否存在异常交易模式；3)检查渠道技术问题；4)评估渠道风险'
        WHEN crd.channel_anomaly_flag IN ('SHARE_SHIFT_ANOMALY', 'HIGH_CONCENTRATION') THEN 
            '建议：1)分析渠道策略变化；2)评估渠道依赖风险；3)建议商户分散渠道使用；4)监控渠道稳定性'
        WHEN crd.channel_anomaly_flag = 'SHARP_DECLINE' THEN 
            '建议：1)了解渠道下降原因；2)检查渠道可用性；3)评估是否需要渠道切换；4)监控后续表现'
        ELSE 
            '建议：保持正常监控，关注后续变化'
    END AS `Recommended Actions`
FROM channel_risk_detection crd
WHERE crd.channel_anomaly_flag != 'NORMAL'
   OR crd.channel_risk_level IN ('CRITICAL_RISK', 'HIGH_RISK')
ORDER BY crd.merchant_id, crd.monthly_channel_volume_usd DESC, crd.month_year DESC;

-- 最终输出3：渠道集中度分析
WITH 
current_month_top_merchants AS (
    SELECT * FROM (
        SELECT 
            t.merchant_id,
            m.merchant_name,
            m.industry,
            m.risk_level,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
            RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
          AND t.status = 'success'
        GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level
    ) s
    WHERE s.current_month_volume_rank <= 20
),
channel_monthly_analysis AS (
    SELECT 
        cmtm.merchant_id,
        cmtm.merchant_name,
        cmtm.industry,
        cmtm.risk_level,
        cmtm.current_month_volume_rank,
        t.channel,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_channel_success_count,
        COUNT(*) AS monthly_channel_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_channel_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_channel_avg_amount_usd,
        RANK() OVER(PARTITION BY t.merchant_id, DATE_FORMAT(t.transaction_date, '%Y-%m') 
                    ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_channel_rank
    FROM current_month_top_merchants cmtm
    JOIN transactions t ON cmtm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY cmtm.merchant_id, cmtm.merchant_name, cmtm.industry, cmtm.risk_level, cmtm.current_month_volume_rank,
             t.channel, DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_base AS (
    SELECT 
        cma.*,
        cma.monthly_channel_volume_usd * 100.0 /
            SUM(cma.monthly_channel_volume_usd) OVER(PARTITION BY cma.merchant_id, cma.month_year) AS monthly_channel_share_percent
    FROM channel_monthly_analysis cma
),
channel_concentration_analysis AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.industry,
        tb.risk_level,
        tb.month_year,
        tb.year_num,
        tb.month_num,
        SUM(POW(tb.monthly_channel_share_percent, 2)) / 100.0 AS monthly_channel_concentration_index,
        COUNT(CASE WHEN tb.monthly_channel_share_percent > 10 THEN 1 END) AS major_channels_count,
        COUNT(CASE WHEN tb.monthly_channel_share_percent > 50 THEN 1 END) AS dominant_channels_count,
        -SUM(CASE WHEN tb.monthly_channel_share_percent > 0 THEN tb.monthly_channel_share_percent * LN(tb.monthly_channel_share_percent / 100.0) ELSE 0 END) AS channel_diversity_index,
        MAX(tb.monthly_channel_share_percent) AS max_channel_share_percent,
        GROUP_CONCAT(CASE WHEN tb.monthly_channel_rank = 1 THEN tb.channel END ORDER BY tb.monthly_channel_share_percent DESC SEPARATOR ', ') AS top_channel_names
    FROM trend_base tb
    GROUP BY tb.merchant_id, tb.merchant_name, tb.industry, tb.risk_level, tb.month_year, tb.year_num, tb.month_num
),
channel_health_assessment AS (
    SELECT 
        cca.merchant_id,
        cca.merchant_name,
        cca.industry,
        cca.risk_level,
        cca.month_year,
        cca.monthly_channel_concentration_index,
        cca.major_channels_count,
        cca.dominant_channels_count,
        cca.channel_diversity_index,
        cca.max_channel_share_percent,
        cca.top_channel_names,
        CASE 
            WHEN cca.monthly_channel_concentration_index > 70 THEN 20
            WHEN cca.monthly_channel_concentration_index > 50 THEN 40
            WHEN cca.monthly_channel_concentration_index > 30 THEN 60
            WHEN cca.monthly_channel_concentration_index > 15 THEN 80
            ELSE 100
        END AS channel_health_score,
        CASE 
            WHEN cca.dominant_channels_count >= 1 AND cca.max_channel_share_percent > 80 THEN 'UNSTABLE'
            WHEN cca.major_channels_count <= 2 THEN 'MODERATE_STABILITY'
            WHEN cca.major_channels_count >= 4 THEN 'HIGH_STABILITY'
            ELSE 'STABLE'
        END AS channel_stability_rating,
        CASE 
            WHEN cca.channel_diversity_index < 0.5 THEN 'LOW_DIVERSITY'
            WHEN cca.channel_diversity_index < 1.0 THEN 'MODERATE_DIVERSITY'
            WHEN cca.channel_diversity_index < 1.5 THEN 'HIGH_DIVERSITY'
            ELSE 'EXCELLENT_DIVERSITY'
        END AS channel_diversity_rating
    FROM channel_concentration_analysis cca
)
SELECT 
    'CHANNEL_CONCENTRATION_ANALYSIS' AS report_type,
    cha.merchant_id AS `Merchant ID`,
    cha.merchant_name AS `Merchant Name`,
    cha.industry AS `Industry`,
    cha.risk_level AS `Original Risk Level`,
    cha.month_year AS `Month-Year`,
    ROUND(cha.monthly_channel_concentration_index, 2) AS `Channel Concentration Index`,
    cha.major_channels_count AS `Major Channels Count`,
    cha.dominant_channels_count AS `Dominant Channels Count`,
    ROUND(cha.channel_diversity_index, 2) AS `Channel Diversity Index`,
    ROUND(cha.max_channel_share_percent, 2) AS `Max Channel Share (%)`,
    cha.top_channel_names AS `Top Channels`,
    cha.channel_health_score AS `Channel Health Score (0-100)`,
    cha.channel_stability_rating AS `Stability Rating`,
    cha.channel_diversity_rating AS `Diversity Rating`,
    CASE 
        WHEN cha.channel_health_score >= 80 THEN '🟢 优秀 - 渠道健康度很高'
        WHEN cha.channel_health_score >= 60 THEN '🟢 良好 - 渠道健康度较好'
        WHEN cha.channel_health_score >= 40 THEN '🟡 一般 - 渠道健康度一般'
        WHEN cha.channel_health_score >= 20 THEN '🟡 较差 - 渠道健康度较低'
        ELSE '🔴 很差 - 渠道健康度极低'
    END AS `Health Assessment`,
    CASE 
        WHEN cha.channel_stability_rating = 'UNSTABLE' THEN '🔴 不稳定 - 存在单点依赖风险'
        WHEN cha.channel_stability_rating = 'MODERATE_STABILITY' THEN '🟡 中等稳定 - 需要关注'
        WHEN cha.channel_stability_rating = 'STABLE' THEN '🟢 稳定 - 基本正常'
        ELSE '🟢 高度稳定 - 非常健康'
    END AS `Stability Assessment`,
    CASE 
        WHEN cha.channel_health_score < 40 OR cha.channel_stability_rating = 'UNSTABLE' THEN 
            '建议：1)建议商户增加渠道多样性；2)避免过度依赖单一渠道；3)建立渠道风险监控；4)制定渠道切换预案'
        WHEN cha.channel_health_score < 60 OR cha.channel_diversity_rating IN ('LOW_DIVERSITY', 'MODERATE_DIVERSITY') THEN 
            '建议：1)评估渠道优化机会；2)考虑增加备用渠道；3)定期回顾渠道策略；4)监控渠道表现'
        ELSE 
            '建议：保持现有渠道策略，定期监控渠道健康度'
    END AS `Recommended Actions`
FROM channel_health_assessment cha
ORDER BY cha.merchant_id, cha.month_year DESC;
