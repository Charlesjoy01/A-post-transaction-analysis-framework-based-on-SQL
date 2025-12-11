-- SQL任务7：商户月度趋势分析
-- 目标：每月固定日期观测商户变动，判断是正常业务增长还是曲折激增

-- 设置查询参数
USE test_database;
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET @target_month = '2025-11';
SET @current_month_start = STR_TO_DATE(CONCAT(@target_month, '-01'), '%Y-%m-%d');
SET @current_month_end = LAST_DAY(@current_month_start);
SET @analysis_date = @current_month_end;
SET @lookback_months = 6;  -- 分析前6个月的历史数据

-- 步骤1：获取当月Top 50商户（按交易金额和笔数分别排名）
WITH current_month_top_merchants AS (
    SELECT 
        t.merchant_id,
        m.merchant_name,
        m.industry,
        m.risk_level,
        m.join_date,
        
        -- 当月交易统计
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
        COUNT(*) AS current_month_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS current_month_avg_amount_usd,
        
        -- 排名（分别按金额和笔数）
        RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank,
        RANK() OVER(ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS current_month_count_rank,
        
        -- 市场份额
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER() AS current_month_market_share_percent
        
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
      AND t.status = 'success'
    GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date
),

-- 步骤2：获取前20名商户的历史6个月数据
selected_top_merchants AS (
    SELECT 
        merchant_id,
        merchant_name,
        industry,
        risk_level,
        join_date,
        current_month_volume_rank,
        current_month_count_rank,
        current_month_volume_usd,
        current_month_success_count
    FROM current_month_top_merchants
    WHERE current_month_volume_rank <= 20 OR current_month_count_rank <= 20
),

-- 步骤3：分析选定商户的历史6个月趋势
historical_monthly_trends AS (
    SELECT 
        stm.merchant_id,
        stm.merchant_name,
        stm.industry,
        stm.risk_level,
        stm.join_date,
        stm.current_month_volume_rank,
        stm.current_month_count_rank,
        stm.current_month_volume_usd,
        stm.current_month_success_count,
        
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        
        -- 月度交易统计
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_success_count,
        COUNT(*) AS monthly_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_avg_amount_usd,
        
        -- 月度排名（相对于所有商户）
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_volume_rank,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS monthly_count_rank,
        
        -- 月度市场份额
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m')) AS monthly_market_share_percent
        
    FROM selected_top_merchants stm
    JOIN transactions t ON stm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY stm.merchant_id, stm.merchant_name, stm.industry, stm.risk_level, stm.join_date,
             stm.current_month_volume_rank, stm.current_month_count_rank,
             stm.current_month_volume_usd, stm.current_month_success_count,
             DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),

-- 步骤4：计算趋势指标和增长率
trend_analysis AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.month_year,
        hmt.year_num,
        hmt.month_num,
        hmt.monthly_volume_usd,
        hmt.monthly_success_count,
        hmt.monthly_avg_amount_usd,
        hmt.monthly_volume_rank,
        hmt.monthly_market_share_percent,
        
        -- 环比增长率（与上月对比）
        CASE 
            WHEN LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_volume_growth_percent,
        
        CASE 
            WHEN LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_success_count - LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_count_growth_percent,
        
        -- 同比增长率（与去年同期对比，如果有数据）
        CASE 
            WHEN LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS yoy_volume_growth_percent,
        
        -- 排名变化
        LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_rank,
        CAST(hmt.monthly_volume_rank AS SIGNED) - CAST(LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS SIGNED) AS rank_change,
        
        -- 市场份额变化
        LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_share,
        hmt.monthly_market_share_percent - LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS share_change_percent
        
    FROM historical_monthly_trends hmt
),

-- 步骤5：计算6个月平均表现和稳定性指标
merchant_performance_summary AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.current_month_volume_rank,
        hmt.current_month_count_rank,
        hmt.current_month_volume_usd,
        hmt.current_month_success_count,
        
        -- 6个月平均表现
        AVG(hmt.monthly_volume_usd) AS avg_6month_volume_usd,
        AVG(hmt.monthly_success_count) AS avg_6month_success_count,
        AVG(hmt.monthly_avg_amount_usd) AS avg_6month_avg_amount_usd,
        AVG(hmt.monthly_volume_rank) AS avg_6month_volume_rank,
        AVG(hmt.monthly_market_share_percent) AS avg_6month_market_share_percent,
        
        -- 稳定性指标（标准差和变异系数）
        STDDEV(hmt.monthly_volume_usd) AS volume_stddev,
        CASE WHEN AVG(hmt.monthly_volume_usd) > 0 
             THEN STDDEV(hmt.monthly_volume_usd) * 100.0 / AVG(hmt.monthly_volume_usd) 
             ELSE NULL 
        END AS volume_cv_percent,  -- 变异系数
        
        STDDEV(hmt.monthly_success_count) AS count_stddev,
        CASE WHEN AVG(hmt.monthly_success_count) > 0 
             THEN STDDEV(hmt.monthly_success_count) * 100.0 / AVG(hmt.monthly_success_count) 
             ELSE NULL 
        END AS count_cv_percent,
        
        -- 趋势稳定性（基于环比增长率的标准差）
        STDDEV(ta.mom_volume_growth_percent) AS growth_volatility,
        
        -- 最新3个月vs前3个月对比
        AVG(CASE WHEN ta.month_num >= MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS recent_3month_avg_volume,
        AVG(CASE WHEN ta.month_num < MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS earlier_3month_avg_volume,
        
        -- 商户年龄（月）
        DATEDIFF(@analysis_date, hmt.join_date) / 30 AS merchant_age_months,
        
        -- 连续增长/下降月数
        COUNT(CASE WHEN ta.mom_volume_growth_percent > 0 THEN 1 END) AS growth_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent < 0 THEN 1 END) AS decline_months
        
    FROM historical_monthly_trends hmt
    JOIN trend_analysis ta ON hmt.merchant_id = ta.merchant_id AND hmt.month_year = ta.month_year
    GROUP BY hmt.merchant_id, hmt.merchant_name, hmt.industry, hmt.risk_level, hmt.join_date,
             hmt.current_month_volume_rank, hmt.current_month_count_rank,
             hmt.current_month_volume_usd, hmt.current_month_success_count
),

-- 步骤6：识别异常增长模式和风险
risk_assessment AS (
    SELECT 
        mps.*,
        
        -- 当前月vs6个月均值对比
        CASE 
            WHEN mps.avg_6month_volume_usd > 0
            THEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / mps.avg_6month_volume_usd
            ELSE NULL
        END AS current_vs_avg_growth_percent,
        
        -- 稳定性评级
        CASE 
            WHEN mps.volume_cv_percent < 20 THEN 'VERY_STABLE'
            WHEN mps.volume_cv_percent < 40 THEN 'STABLE'
            WHEN mps.volume_cv_percent < 60 THEN 'MODERATE_VOLATILITY'
            WHEN mps.volume_cv_percent < 80 THEN 'HIGH_VOLATILITY'
            ELSE 'EXTREME_VOLATILITY'
        END AS stability_rating,
        
        -- 增长模式识别
        CASE 
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 AND mps.volume_cv_percent > 80 THEN 'EXPLOSIVE_GROWTH_HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 AND mps.volume_cv_percent > 60 THEN 'RAPID_GROWTH_MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 AND mps.volume_cv_percent < 40 THEN 'STEADY_GROWTH_LOW_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) BETWEEN -20 AND 20 AND mps.volume_cv_percent < 30 THEN 'STABLE_BUSINESS'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -50 THEN 'SHARP_DECLINE'
            ELSE 'NORMAL_VARIATION'
        END AS growth_pattern_type,
        
        -- 综合风险评级
        CASE 
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 OR mps.volume_cv_percent > 100) AND mps.merchant_age_months < 6 THEN 'CRITICAL_RISK'
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 OR mps.volume_cv_percent > 80) AND mps.risk_level = 'high' THEN 'HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 OR mps.volume_cv_percent > 60 THEN 'MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -30 OR mps.volume_cv_percent > 40 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS overall_risk_level
        
    FROM merchant_performance_summary mps
)

-- 最终输出1：当月Top 20商户表现
SELECT 
    'CURRENT_MONTH_TOP_MERCHANTS' AS report_type,
    mps.current_month_volume_rank AS "Volume Rank",
    mps.current_month_count_rank AS "Count Rank",
    mps.merchant_id AS "Merchant ID",
    mps.merchant_name AS "Merchant Name",
    mps.industry AS "Industry",
    mps.risk_level AS "Original Risk Level",
    ROUND(mps.current_month_volume_usd, 2) AS "Current Month Volume (USD)",
    mps.current_month_success_count AS "Current Month Success Count",
    ROUND(mps.avg_6month_volume_usd, 2) AS "6-Month Avg Volume (USD)",
    ROUND(mps.avg_6month_success_count, 0) AS "6-Month Avg Count",
    ROUND((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0), 2) AS "Growth vs 6M Avg (%)",
    ROUND(mps.volume_cv_percent, 2) AS "Volume Volatility (%)",
    ROUND(mps.merchant_age_months, 1) AS "Merchant Age (Months)",
    
    mps.stability_rating AS "Stability Rating",
    mps.growth_pattern_type AS "Growth Pattern",
    mps.overall_risk_level AS "Risk Level",
    
    CASE 
        WHEN mps.overall_risk_level = 'CRITICAL_RISK' THEN '🔴 需要立即关注和深度调查'
        WHEN mps.overall_risk_level = 'HIGH_RISK' THEN '🔴 需要加强监控和调查'
        WHEN mps.overall_risk_level = 'MEDIUM_RISK' THEN '🟡 需要密切关注和定期评估'
        WHEN mps.overall_risk_level = 'LOW_MEDIUM_RISK' THEN '🟢 保持正常监控'
        ELSE '✅ 正常业务表现'
    END AS "Risk Assessment Note"
    
FROM risk_assessment mps
WHERE mps.current_month_volume_rank <= 20 OR mps.current_month_count_rank <= 20
ORDER BY mps.current_month_volume_rank, mps.current_month_count_rank;

-- 最终输出2：增长模式异常识别
WITH
current_month_top_merchants AS (
    SELECT 
        t.merchant_id,
        m.merchant_name,
        m.industry,
        m.risk_level,
        m.join_date,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
        COUNT(*) AS current_month_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS current_month_avg_amount_usd,
        RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank,
        RANK() OVER(ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS current_month_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER() AS current_month_market_share_percent
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
      AND t.status = 'success'
    GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date
),
selected_top_merchants AS (
    SELECT 
        merchant_id,
        merchant_name,
        industry,
        risk_level,
        join_date,
        current_month_volume_rank,
        current_month_count_rank,
        current_month_volume_usd,
        current_month_success_count
    FROM current_month_top_merchants
    WHERE current_month_volume_rank <= 20 OR current_month_count_rank <= 20
),
historical_monthly_trends AS (
    SELECT 
        stm.merchant_id,
        stm.merchant_name,
        stm.industry,
        stm.risk_level,
        stm.join_date,
        stm.current_month_volume_rank,
        stm.current_month_count_rank,
        stm.current_month_volume_usd,
        stm.current_month_success_count,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_success_count,
        COUNT(*) AS monthly_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_avg_amount_usd,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_volume_rank,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS monthly_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m')) AS monthly_market_share_percent
    FROM selected_top_merchants stm
    JOIN transactions t ON stm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY stm.merchant_id, stm.merchant_name, stm.industry, stm.risk_level, stm.join_date,
             stm.current_month_volume_rank, stm.current_month_count_rank,
             stm.current_month_volume_usd, stm.current_month_success_count,
             DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_analysis AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.month_year,
        hmt.year_num,
        hmt.month_num,
        hmt.monthly_volume_usd,
        hmt.monthly_success_count,
        hmt.monthly_avg_amount_usd,
        hmt.monthly_volume_rank,
        hmt.monthly_market_share_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_volume_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_success_count - LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_count_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS yoy_volume_growth_percent,
        LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_rank,
        CAST(hmt.monthly_volume_rank AS SIGNED) - CAST(LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS SIGNED) AS rank_change,
        LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_share,
        hmt.monthly_market_share_percent - LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS share_change_percent
    FROM historical_monthly_trends hmt
),
merchant_performance_summary AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.current_month_volume_rank,
        hmt.current_month_count_rank,
        hmt.current_month_volume_usd,
        hmt.current_month_success_count,
        AVG(hmt.monthly_volume_usd) AS avg_6month_volume_usd,
        AVG(hmt.monthly_success_count) AS avg_6month_success_count,
        AVG(hmt.monthly_avg_amount_usd) AS avg_6month_avg_amount_usd,
        AVG(hmt.monthly_volume_rank) AS avg_6month_volume_rank,
        AVG(hmt.monthly_market_share_percent) AS avg_6month_market_share_percent,
        STDDEV(hmt.monthly_volume_usd) AS volume_stddev,
        CASE WHEN AVG(hmt.monthly_volume_usd) > 0 
             THEN STDDEV(hmt.monthly_volume_usd) * 100.0 / AVG(hmt.monthly_volume_usd) 
             ELSE NULL 
        END AS volume_cv_percent,
        STDDEV(hmt.monthly_success_count) AS count_stddev,
        CASE WHEN AVG(hmt.monthly_success_count) > 0 
             THEN STDDEV(hmt.monthly_success_count) * 100.0 / AVG(hmt.monthly_success_count) 
             ELSE NULL 
        END AS count_cv_percent,
        STDDEV(ta.mom_volume_growth_percent) AS growth_volatility,
        AVG(CASE WHEN ta.month_num >= MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS recent_3month_avg_volume,
        AVG(CASE WHEN ta.month_num < MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS earlier_3month_avg_volume,
        DATEDIFF(@analysis_date, hmt.join_date) / 30 AS merchant_age_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent > 0 THEN 1 END) AS growth_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent < 0 THEN 1 END) AS decline_months
    FROM historical_monthly_trends hmt
    JOIN trend_analysis ta ON hmt.merchant_id = ta.merchant_id AND hmt.month_year = ta.month_year
    GROUP BY hmt.merchant_id, hmt.merchant_name, hmt.industry, hmt.risk_level, hmt.join_date,
             hmt.current_month_volume_rank, hmt.current_month_count_rank,
             hmt.current_month_volume_usd, hmt.current_month_success_count
),
risk_assessment AS (
    SELECT 
        mps.*,
        CASE 
            WHEN mps.avg_6month_volume_usd > 0
            THEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / mps.avg_6month_volume_usd
            ELSE NULL
        END AS current_vs_avg_growth_percent,
        CASE 
            WHEN mps.volume_cv_percent < 20 THEN 'VERY_STABLE'
            WHEN mps.volume_cv_percent < 40 THEN 'STABLE'
            WHEN mps.volume_cv_percent < 60 THEN 'MODERATE_VOLATILITY'
            WHEN mps.volume_cv_percent < 80 THEN 'HIGH_VOLATILITY'
            ELSE 'EXTREME_VOLATILITY'
        END AS stability_rating,
        CASE 
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 AND mps.volume_cv_percent > 80 THEN 'EXPLOSIVE_GROWTH_HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 AND mps.volume_cv_percent > 60 THEN 'RAPID_GROWTH_MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 AND mps.volume_cv_percent < 40 THEN 'STEADY_GROWTH_LOW_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) BETWEEN -20 AND 20 AND mps.volume_cv_percent < 30 THEN 'STABLE_BUSINESS'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -50 THEN 'SHARP_DECLINE'
            ELSE 'NORMAL_VARIATION'
        END AS growth_pattern_type,
        CASE 
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 OR mps.volume_cv_percent > 100) AND mps.merchant_age_months < 6 THEN 'CRITICAL_RISK'
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 OR mps.volume_cv_percent > 80) AND mps.risk_level = 'high' THEN 'HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 OR mps.volume_cv_percent > 60 THEN 'MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -30 OR mps.volume_cv_percent > 40 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS overall_risk_level
    FROM merchant_performance_summary mps
)
SELECT 
    'ABNORMAL_GROWTH_PATTERNS' AS report_type,
    mps.merchant_id AS "Merchant ID",
    mps.merchant_name AS "Merchant Name",
    mps.industry AS "Industry",
    mps.risk_level AS "Original Risk Level",
    ROUND(mps.current_month_volume_usd, 2) AS "Current Month Volume (USD)",
    ROUND(mps.avg_6month_volume_usd, 2) AS "6-Month Avg Volume (USD)",
    ROUND((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0), 2) AS "Growth vs 6M Avg (%)",
    ROUND(mps.volume_cv_percent, 2) AS "Volume Volatility (%)",
    ROUND(mps.merchant_age_months, 1) AS "Merchant Age (Months)",
    mps.growth_months AS "Growth Months",
    mps.decline_months AS "Decline Months",
    
    mps.growth_pattern_type AS "Growth Pattern Type",
    mps.overall_risk_level AS "Risk Level",
    
    CASE 
        WHEN mps.growth_pattern_type = 'EXPLOSIVE_GROWTH_HIGH_RISK' THEN '🔴 爆发式增长 - 高度可疑'
        WHEN mps.growth_pattern_type = 'RAPID_GROWTH_MEDIUM_RISK' THEN '🟡 快速增长 - 需要关注'
        WHEN mps.growth_pattern_type = 'SHARP_DECLINE' THEN '🔻 急剧下降 - 业务异常'
        WHEN mps.growth_pattern_type = 'STEADY_GROWTH_LOW_RISK' THEN '🟢 稳定增长 - 正常业务'
        ELSE '➖ 正常波动 - 无需特别关注'
    END AS "Pattern Assessment",
    
    CASE 
        WHEN mps.growth_pattern_type IN ('EXPLOSIVE_GROWTH_HIGH_RISK', 'RAPID_GROWTH_MEDIUM_RISK') THEN 
            '建议：1)立即联系商户了解业务增长原因；2)要求提供业务增长支撑材料；3)分析用户增长是否匹配；4)检查是否存在异常交易模式'
        WHEN mps.growth_pattern_type = 'SHARP_DECLINE' THEN 
            '建议：1)了解业务下降原因；2)检查是否存在合规问题；3)评估商户持续经营能力；4)考虑风险管控措施'
        ELSE 
            '建议：保持正常监控频率，定期回顾业务表现'
    END AS "Recommended Actions"
    
FROM risk_assessment mps
WHERE mps.growth_pattern_type NOT IN ('STABLE_BUSINESS', 'NORMAL_VARIATION')
   OR mps.overall_risk_level IN ('CRITICAL_RISK', 'HIGH_RISK')
ORDER BY (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) DESC, mps.volume_cv_percent DESC;

-- 最终输出3：排名变化分析（新进入Top 20的商户）
WITH
current_month_top_merchants AS (
    SELECT 
        t.merchant_id,
        m.merchant_name,
        m.industry,
        m.risk_level,
        m.join_date,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
        COUNT(*) AS current_month_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS current_month_avg_amount_usd,
        RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank,
        RANK() OVER(ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS current_month_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER() AS current_month_market_share_percent
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
      AND t.status = 'success'
    GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date
),
selected_top_merchants AS (
    SELECT 
        merchant_id,
        merchant_name,
        industry,
        risk_level,
        join_date,
        current_month_volume_rank,
        current_month_count_rank,
        current_month_volume_usd,
        current_month_success_count
    FROM current_month_top_merchants
    WHERE current_month_volume_rank <= 20 OR current_month_count_rank <= 20
),
historical_monthly_trends AS (
    SELECT 
        stm.merchant_id,
        stm.merchant_name,
        stm.industry,
        stm.risk_level,
        stm.join_date,
        stm.current_month_volume_rank,
        stm.current_month_count_rank,
        stm.current_month_volume_usd,
        stm.current_month_success_count,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_success_count,
        COUNT(*) AS monthly_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_avg_amount_usd,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_volume_rank,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS monthly_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m')) AS monthly_market_share_percent
    FROM selected_top_merchants stm
    JOIN transactions t ON stm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY stm.merchant_id, stm.merchant_name, stm.industry, stm.risk_level, stm.join_date,
             stm.current_month_volume_rank, stm.current_month_count_rank,
             stm.current_month_volume_usd, stm.current_month_success_count,
             DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_analysis AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.month_year,
        hmt.year_num,
        hmt.month_num,
        hmt.monthly_volume_usd,
        hmt.monthly_success_count,
        hmt.monthly_avg_amount_usd,
        hmt.monthly_volume_rank,
        hmt.monthly_market_share_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_volume_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_success_count - LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_count_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS yoy_volume_growth_percent,
        LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_rank,
        CAST(hmt.monthly_volume_rank AS SIGNED) - CAST(LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS SIGNED) AS rank_change,
        LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_share,
        hmt.monthly_market_share_percent - LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS share_change_percent
    FROM historical_monthly_trends hmt
),
merchant_performance_summary AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.current_month_volume_rank,
        hmt.current_month_count_rank,
        hmt.current_month_volume_usd,
        hmt.current_month_success_count,
        AVG(hmt.monthly_volume_usd) AS avg_6month_volume_usd,
        AVG(hmt.monthly_success_count) AS avg_6month_success_count,
        AVG(hmt.monthly_avg_amount_usd) AS avg_6month_avg_amount_usd,
        AVG(hmt.monthly_volume_rank) AS avg_6month_volume_rank,
        AVG(hmt.monthly_market_share_percent) AS avg_6month_market_share_percent,
        STDDEV(hmt.monthly_volume_usd) AS volume_stddev,
        CASE WHEN AVG(hmt.monthly_volume_usd) > 0 
             THEN STDDEV(hmt.monthly_volume_usd) * 100.0 / AVG(hmt.monthly_volume_usd) 
             ELSE NULL 
        END AS volume_cv_percent,
        STDDEV(hmt.monthly_success_count) AS count_stddev,
        CASE WHEN AVG(hmt.monthly_success_count) > 0 
             THEN STDDEV(hmt.monthly_success_count) * 100.0 / AVG(hmt.monthly_success_count) 
             ELSE NULL 
        END AS count_cv_percent,
        STDDEV(ta.mom_volume_growth_percent) AS growth_volatility,
        AVG(CASE WHEN ta.month_num >= MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS recent_3month_avg_volume,
        AVG(CASE WHEN ta.month_num < MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS earlier_3month_avg_volume,
        DATEDIFF(@analysis_date, hmt.join_date) / 30 AS merchant_age_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent > 0 THEN 1 END) AS growth_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent < 0 THEN 1 END) AS decline_months
    FROM historical_monthly_trends hmt
    JOIN trend_analysis ta ON hmt.merchant_id = ta.merchant_id AND hmt.month_year = ta.month_year
    GROUP BY hmt.merchant_id, hmt.merchant_name, hmt.industry, hmt.risk_level, hmt.join_date,
             hmt.current_month_volume_rank, hmt.current_month_count_rank,
             hmt.current_month_volume_usd, hmt.current_month_success_count
),
risk_assessment AS (
    SELECT 
        mps.*,
        CASE 
            WHEN mps.avg_6month_volume_usd > 0
            THEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / mps.avg_6month_volume_usd
            ELSE NULL
        END AS current_vs_avg_growth_percent,
        CASE 
            WHEN mps.volume_cv_percent < 20 THEN 'VERY_STABLE'
            WHEN mps.volume_cv_percent < 40 THEN 'STABLE'
            WHEN mps.volume_cv_percent < 60 THEN 'MODERATE_VOLATILITY'
            WHEN mps.volume_cv_percent < 80 THEN 'HIGH_VOLATILITY'
            ELSE 'EXTREME_VOLATILITY'
        END AS stability_rating,
        CASE 
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 AND mps.volume_cv_percent > 80 THEN 'EXPLOSIVE_GROWTH_HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 AND mps.volume_cv_percent > 60 THEN 'RAPID_GROWTH_MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 AND mps.volume_cv_percent < 40 THEN 'STEADY_GROWTH_LOW_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) BETWEEN -20 AND 20 AND mps.volume_cv_percent < 30 THEN 'STABLE_BUSINESS'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -50 THEN 'SHARP_DECLINE'
            ELSE 'NORMAL_VARIATION'
        END AS growth_pattern_type,
        CASE 
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 200 OR mps.volume_cv_percent > 100) AND mps.merchant_age_months < 6 THEN 'CRITICAL_RISK'
            WHEN ((mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 100 OR mps.volume_cv_percent > 80) AND mps.risk_level = 'high' THEN 'HIGH_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) > 50 OR mps.volume_cv_percent > 60 THEN 'MEDIUM_RISK'
            WHEN (mps.current_month_volume_usd - mps.avg_6month_volume_usd) * 100.0 / NULLIF(mps.avg_6month_volume_usd, 0) < -30 OR mps.volume_cv_percent > 40 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS overall_risk_level
    FROM merchant_performance_summary mps
),
ranking_changes AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.current_month_volume_rank,
        
        -- 上个月排名
        MAX(CASE WHEN hmt.month_year = DATE_FORMAT(DATE_SUB(@current_month_start, INTERVAL 1 MONTH), '%Y-%m') THEN hmt.monthly_volume_rank END) AS previous_month_rank,
        
        -- 排名变化
        CAST(hmt.current_month_volume_rank AS SIGNED) - 
        CAST(MAX(CASE WHEN hmt.month_year = DATE_FORMAT(DATE_SUB(@current_month_start, INTERVAL 1 MONTH), '%Y-%m') THEN hmt.monthly_volume_rank END) AS SIGNED) AS rank_change,
        
        -- 当前月数据
        MAX(hmt.current_month_volume_usd) AS current_month_volume_usd,
        MAX(hmt.current_month_success_count) AS current_month_success_count
        
    FROM historical_monthly_trends hmt
    GROUP BY hmt.merchant_id, hmt.merchant_name, hmt.industry, hmt.risk_level, hmt.current_month_volume_rank
)
SELECT 
    'RANKING_CHANGES' AS report_type,
    rc.merchant_id AS "Merchant ID",
    rc.merchant_name AS "Merchant Name",
    rc.industry AS "Industry",
    rc.risk_level AS "Original Risk Level",
    rc.previous_month_rank AS "Previous Month Rank",
    rc.current_month_volume_rank AS "Current Month Rank",
    rc.rank_change AS "Rank Change",
    ROUND(rc.current_month_volume_usd, 2) AS "Current Month Volume (USD)",
    rc.current_month_success_count AS "Current Month Success Count",
    
    CASE 
        WHEN rc.rank_change < -10 THEN '🔺 排名大幅提升 - 需要关注'
        WHEN rc.rank_change < -5 THEN '🔼 排名明显提升 - 值得注意'
        WHEN rc.rank_change > 10 THEN '🔻 排名大幅下降 - 业务异常'
        WHEN rc.rank_change > 5 THEN '🔽 排名明显下降 - 需要了解原因'
        WHEN rc.previous_month_rank IS NULL AND rc.current_month_volume_rank <= 20 THEN '🆕 新进入Top 20'
        ELSE '➖ 排名变化不大'
    END AS "Ranking Change Assessment",
    
    CASE 
        WHEN rc.rank_change < -10 OR (rc.previous_month_rank IS NULL AND rc.current_month_volume_rank <= 20) THEN 
            '建议：1)立即联系商户了解业务变化原因；2)分析是否存在异常交易模式；3)检查用户增长是否匹配；4)评估业务可持续性'
        WHEN rc.rank_change > 10 THEN 
            '建议：1)了解业务下降原因；2)检查是否存在合规问题；3)评估商户持续经营能力；4)考虑风险管控措施'
        ELSE 
            '建议：保持正常监控，关注后续变化'
    END AS "Recommended Actions"
    
FROM ranking_changes rc
WHERE rc.rank_change IS NOT NULL 
   AND (ABS(rc.rank_change) >= 5 OR (rc.previous_month_rank IS NULL AND rc.current_month_volume_rank <= 20))
ORDER BY ABS(rc.rank_change) DESC, rc.current_month_volume_rank;

-- 最终输出4：稳定性分析（高波动性商户）
WITH
current_month_top_merchants AS (
    SELECT 
        t.merchant_id,
        m.merchant_name,
        m.industry,
        m.risk_level,
        m.join_date,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS current_month_success_count,
        COUNT(*) AS current_month_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS current_month_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS current_month_avg_amount_usd,
        RANK() OVER(ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS current_month_volume_rank,
        RANK() OVER(ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS current_month_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER() AS current_month_market_share_percent
    FROM transactions t
    JOIN merchants m ON t.merchant_id = m.merchant_id
    WHERE t.transaction_date BETWEEN @current_month_start AND @current_month_end
      AND t.status = 'success'
    GROUP BY t.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date
),
selected_top_merchants AS (
    SELECT 
        merchant_id,
        merchant_name,
        industry,
        risk_level,
        join_date,
        current_month_volume_rank,
        current_month_count_rank,
        current_month_volume_usd,
        current_month_success_count
    FROM current_month_top_merchants
    WHERE current_month_volume_rank <= 20 OR current_month_count_rank <= 20
),
historical_monthly_trends AS (
    SELECT 
        stm.merchant_id,
        stm.merchant_name,
        stm.industry,
        stm.risk_level,
        stm.join_date,
        stm.current_month_volume_rank,
        stm.current_month_count_rank,
        stm.current_month_volume_usd,
        stm.current_month_success_count,
        DATE_FORMAT(t.transaction_date, '%Y-%m') AS month_year,
        YEAR(t.transaction_date) AS year_num,
        MONTH(t.transaction_date) AS month_num,
        COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS monthly_success_count,
        COUNT(*) AS monthly_total_count,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS monthly_volume_usd,
        AVG(CASE WHEN t.status = 'success' THEN t.amount_usd END) AS monthly_avg_amount_usd,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) DESC) AS monthly_volume_rank,
        RANK() OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m') ORDER BY COUNT(CASE WHEN t.status = 'success' THEN 1 END) DESC) AS monthly_count_rank,
        SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) * 100.0 / 
            SUM(SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END)) OVER(PARTITION BY DATE_FORMAT(t.transaction_date, '%Y-%m')) AS monthly_market_share_percent
    FROM selected_top_merchants stm
    JOIN transactions t ON stm.merchant_id = t.merchant_id
    WHERE t.transaction_date >= DATE_SUB(@current_month_start, INTERVAL @lookback_months MONTH)
      AND t.transaction_date < @current_month_start
      AND t.status = 'success'
    GROUP BY stm.merchant_id, stm.merchant_name, stm.industry, stm.risk_level, stm.join_date,
             stm.current_month_volume_rank, stm.current_month_count_rank,
             stm.current_month_volume_usd, stm.current_month_success_count,
             DATE_FORMAT(t.transaction_date, '%Y-%m'), YEAR(t.transaction_date), MONTH(t.transaction_date)
),
trend_analysis AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.month_year,
        hmt.year_num,
        hmt.month_num,
        hmt.monthly_volume_usd,
        hmt.monthly_success_count,
        hmt.monthly_avg_amount_usd,
        hmt.monthly_volume_rank,
        hmt.monthly_market_share_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_volume_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_success_count - LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_success_count) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS mom_count_growth_percent,
        CASE 
            WHEN LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) > 0
            THEN (hmt.monthly_volume_usd - LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)) * 100.0 /
                 LAG(hmt.monthly_volume_usd, 12) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num)
            ELSE NULL
        END AS yoy_volume_growth_percent,
        LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_rank,
        CAST(hmt.monthly_volume_rank AS SIGNED) - CAST(LAG(hmt.monthly_volume_rank) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS SIGNED) AS rank_change,
        LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS previous_month_share,
        hmt.monthly_market_share_percent - LAG(hmt.monthly_market_share_percent) OVER(PARTITION BY hmt.merchant_id ORDER BY hmt.year_num, hmt.month_num) AS share_change_percent
    FROM historical_monthly_trends hmt
),
merchant_performance_summary AS (
    SELECT 
        hmt.merchant_id,
        hmt.merchant_name,
        hmt.industry,
        hmt.risk_level,
        hmt.current_month_volume_rank,
        hmt.current_month_count_rank,
        hmt.current_month_volume_usd,
        hmt.current_month_success_count,
        AVG(hmt.monthly_volume_usd) AS avg_6month_volume_usd,
        AVG(hmt.monthly_success_count) AS avg_6month_success_count,
        AVG(hmt.monthly_avg_amount_usd) AS avg_6month_avg_amount_usd,
        AVG(hmt.monthly_volume_rank) AS avg_6month_volume_rank,
        AVG(hmt.monthly_market_share_percent) AS avg_6month_market_share_percent,
        STDDEV(hmt.monthly_volume_usd) AS volume_stddev,
        CASE WHEN AVG(hmt.monthly_volume_usd) > 0 
             THEN STDDEV(hmt.monthly_volume_usd) * 100.0 / AVG(hmt.monthly_volume_usd) 
             ELSE NULL 
        END AS volume_cv_percent,
        STDDEV(hmt.monthly_success_count) AS count_stddev,
        CASE WHEN AVG(hmt.monthly_success_count) > 0 
             THEN STDDEV(hmt.monthly_success_count) * 100.0 / AVG(hmt.monthly_success_count) 
             ELSE NULL 
        END AS count_cv_percent,
        STDDEV(ta.mom_volume_growth_percent) AS growth_volatility,
        AVG(CASE WHEN ta.month_num >= MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS recent_3month_avg_volume,
        AVG(CASE WHEN ta.month_num < MONTH(@analysis_date) - 2 THEN ta.monthly_volume_usd END) AS earlier_3month_avg_volume,
        DATEDIFF(@analysis_date, hmt.join_date) / 30 AS merchant_age_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent > 0 THEN 1 END) AS growth_months,
        COUNT(CASE WHEN ta.mom_volume_growth_percent < 0 THEN 1 END) AS decline_months
    FROM historical_monthly_trends hmt
    JOIN trend_analysis ta ON hmt.merchant_id = ta.merchant_id AND hmt.month_year = ta.month_year
    GROUP BY hmt.merchant_id, hmt.merchant_name, hmt.industry, hmt.risk_level, hmt.join_date,
             hmt.current_month_volume_rank, hmt.current_month_count_rank,
             hmt.current_month_volume_usd, hmt.current_month_success_count
)
SELECT 
    'STABILITY_ANALYSIS' AS report_type,
    mps.merchant_id AS "Merchant ID",
    mps.merchant_name AS "Merchant Name",
    mps.industry AS "Industry",
    mps.risk_level AS "Original Risk Level",
    ROUND(mps.volume_cv_percent, 2) AS "Volume Volatility (%)",
    ROUND(mps.count_cv_percent, 2) AS "Count Volatility (%)",
    ROUND(mps.growth_volatility, 2) AS "Growth Volatility",
    CASE 
        WHEN mps.volume_cv_percent < 20 THEN 'VERY_STABLE'
        WHEN mps.volume_cv_percent < 40 THEN 'STABLE'
        WHEN mps.volume_cv_percent < 60 THEN 'MODERATE_VOLATILITY'
        WHEN mps.volume_cv_percent < 80 THEN 'HIGH_VOLATILITY'
        ELSE 'EXTREME_VOLATILITY'
    END AS "Stability Rating",
    ROUND(mps.avg_6month_volume_usd, 2) AS "6-Month Avg Volume (USD)",
    ROUND(mps.merchant_age_months, 1) AS "Merchant Age (Months)",
    mps.growth_months AS "Growth Months",
    mps.decline_months AS "Decline Months",
    
    CASE 
        WHEN mps.volume_cv_percent > 100 THEN '🔴 极高波动性 - 业务极不稳定'
        WHEN mps.volume_cv_percent > 80 THEN '🔴 高波动性 - 业务不稳定'
        WHEN mps.volume_cv_percent > 60 THEN '🟡 中高波动性 - 需要关注'
        WHEN mps.volume_cv_percent > 40 THEN '🟡 中等波动性 - 值得注意'
        WHEN mps.volume_cv_percent > 20 THEN '🟢 低波动性 - 相对稳定'
        ELSE '✅ 极低波动性 - 非常稳定'
    END AS "Stability Assessment",
    
    CASE 
        WHEN mps.volume_cv_percent > 80 THEN 
            '建议：1)深入分析业务波动原因；2)检查是否存在季节性因素；3)评估商户风险管理能力；4)考虑设置交易限制；5)增加监控频率'
        WHEN mps.volume_cv_percent > 60 THEN 
            '建议：1)定期回顾业务表现；2)了解波动原因；3)评估业务稳定性；4)保持密切关注'
        ELSE 
            '建议：保持正常监控频率，定期评估'
    END AS "Recommended Actions"
    
FROM merchant_performance_summary mps
WHERE mps.volume_cv_percent > 60  -- 只显示高波动性商户
ORDER BY mps.volume_cv_percent DESC, mps.merchant_age_months;
