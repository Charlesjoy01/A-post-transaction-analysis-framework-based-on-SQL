-- SQL任务2：商户趋势分析（周维度）
-- 目标：观察7天内的商户变动趋势，找出Top 20商户

-- 设置查询参数
SET @analysis_date = CURRENT_DATE();
SET @start_date = DATE_SUB(@analysis_date, INTERVAL 7 DAY);
SET SESSION net_read_timeout = 600;
SET SESSION net_write_timeout = 600;
SET SESSION max_execution_time = 300000;

-- 查询1：日交易总金额前20名商户
WITH daily_top_merchants_by_volume AS (
    SELECT 
        x.transaction_date,
        x.merchant_id,
        x.merchant_name,
        x.industry,
        x.daily_volume_usd,
        x.daily_success_count,
        x.daily_total_count,
        x.daily_volume_usd * 100.0 / NULLIF(SUM(x.daily_volume_usd) OVER(PARTITION BY x.transaction_date), 0) AS market_share_percent,
        RANK() OVER(PARTITION BY x.transaction_date ORDER BY x.daily_volume_usd DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) x
),

-- 查询2：日交易总笔数前20名商户
 daily_top_merchants_by_count AS (
    SELECT 
        y.transaction_date,
        y.merchant_id,
        y.merchant_name,
        y.industry,
        y.daily_success_count,
        y.daily_total_count,
        y.daily_volume_usd,
        RANK() OVER(PARTITION BY y.transaction_date ORDER BY y.daily_success_count DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) y
),

trend_base AS (
    SELECT 
        merchant_id,
        merchant_name,
        AVG(CASE WHEN transaction_date >= DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_volume_usd END) AS recent_3day_avg_volume,
        AVG(CASE WHEN transaction_date >= DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_success_count END) AS recent_3day_avg_count,
        AVG(CASE WHEN transaction_date < DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_volume_usd END) AS earlier_4day_avg_volume,
        AVG(CASE WHEN transaction_date < DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_success_count END) AS earlier_4day_avg_count,
        AVG(CASE WHEN transaction_date = @analysis_date THEN daily_rank END) AS current_rank,
        AVG(daily_rank) AS avg_7day_rank
    FROM daily_top_merchants_by_volume
    GROUP BY merchant_id, merchant_name
),
trend_analysis AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.recent_3day_avg_volume,
        tb.recent_3day_avg_count,
        tb.earlier_4day_avg_volume,
        tb.earlier_4day_avg_count,
        CASE 
            WHEN tb.earlier_4day_avg_volume > 0
            THEN (tb.recent_3day_avg_volume - tb.earlier_4day_avg_volume) * 100.0 / tb.earlier_4day_avg_volume
            ELSE NULL
        END AS volume_trend_percent,
        tb.current_rank,
        tb.avg_7day_rank
    FROM trend_base tb
)

-- 最终输出1：按交易金额排名的Top 20商户
SELECT 
    'TOP_BY_VOLUME' AS ranking_type,
    v.transaction_date AS "Date",
    v.merchant_id AS "Merchant ID",
    v.merchant_name AS "Merchant Name",
    v.daily_rank AS "Daily Rank",
    ROUND(v.daily_volume_usd, 2) AS "Daily Volume (USD)",
    v.daily_success_count AS "Success Count",
    v.daily_total_count AS "Total Count",
    ROUND(v.market_share_percent, 2) AS "Market Share (%)",
    
    -- 趋势分析
    CASE 
        WHEN t.volume_trend_percent > 20 THEN 'RISING'
        WHEN t.volume_trend_percent < -20 THEN 'DECLINING'
        WHEN t.volume_trend_percent BETWEEN -20 AND 20 THEN 'STABLE'
        ELSE 'NEW_OR_DATA_INSUFFICIENT'
    END AS "Trend Direction",
    ROUND(t.volume_trend_percent, 2) AS "Volume Trend (%)",
    
    -- 排名变化
    CASE 
        WHEN t.current_rank < t.avg_7day_rank THEN 'IMPROVING'
        WHEN t.current_rank > t.avg_7day_rank THEN 'DECLINING'
        ELSE 'STABLE'
    END AS "Rank Trend",
    t.current_rank AS "Current Rank",
    ROUND(t.avg_7day_rank, 1) AS "7-Day Avg Rank"
    
FROM daily_top_merchants_by_volume v
LEFT JOIN trend_analysis t ON v.merchant_id = t.merchant_id
WHERE v.daily_rank <= 20
ORDER BY v.transaction_date DESC, v.daily_rank;

-- 最终输出2：按交易笔数排名的Top 20商户
WITH 
daily_top_merchants_by_volume AS (
    SELECT 
        x.transaction_date,
        x.merchant_id,
        x.merchant_name,
        x.industry,
        x.daily_volume_usd,
        x.daily_success_count,
        x.daily_total_count,
        x.daily_volume_usd * 100.0 / NULLIF(SUM(x.daily_volume_usd) OVER(PARTITION BY x.transaction_date), 0) AS market_share_percent,
        RANK() OVER(PARTITION BY x.transaction_date ORDER BY x.daily_volume_usd DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) x
),
daily_top_merchants_by_count AS (
    SELECT 
        y.transaction_date,
        y.merchant_id,
        y.merchant_name,
        y.industry,
        y.daily_success_count,
        y.daily_total_count,
        y.daily_volume_usd,
        RANK() OVER(PARTITION BY y.transaction_date ORDER BY y.daily_success_count DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) y
),
trend_base AS (
    SELECT 
        merchant_id,
        merchant_name,
        AVG(CASE WHEN transaction_date >= DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_volume_usd END) AS recent_3day_avg_volume,
        AVG(CASE WHEN transaction_date >= DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_success_count END) AS recent_3day_avg_count,
        AVG(CASE WHEN transaction_date < DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_volume_usd END) AS earlier_4day_avg_volume,
        AVG(CASE WHEN transaction_date < DATE_SUB(@analysis_date, INTERVAL 3 DAY) THEN daily_success_count END) AS earlier_4day_avg_count,
        AVG(CASE WHEN transaction_date = @analysis_date THEN daily_rank END) AS current_rank,
        AVG(daily_rank) AS avg_7day_rank
    FROM daily_top_merchants_by_volume
    GROUP BY merchant_id, merchant_name
),
trend_analysis AS (
    SELECT 
        tb.merchant_id,
        tb.merchant_name,
        tb.recent_3day_avg_volume,
        tb.recent_3day_avg_count,
        tb.earlier_4day_avg_volume,
        tb.earlier_4day_avg_count,
        CASE 
            WHEN tb.earlier_4day_avg_volume > 0
            THEN (tb.recent_3day_avg_volume - tb.earlier_4day_avg_volume) * 100.0 / tb.earlier_4day_avg_volume
            ELSE NULL
        END AS volume_trend_percent,
        tb.current_rank,
        tb.avg_7day_rank
    FROM trend_base tb
)
SELECT 
    'TOP_BY_COUNT' AS ranking_type,
    c.transaction_date AS "Date",
    c.merchant_id AS "Merchant ID",
    c.merchant_name AS "Merchant Name",
    c.daily_rank AS "Daily Rank",
    c.daily_success_count AS "Success Count",
    c.daily_total_count AS "Total Count",
    ROUND(c.daily_volume_usd, 2) AS "Daily Volume (USD)",
    ROUND(c.daily_volume_usd / NULLIF(c.daily_success_count, 0), 2) AS "Avg Transaction Value (USD)",
    
    -- 趋势分析
    CASE 
        WHEN t.volume_trend_percent > 20 THEN 'RISING'
        WHEN t.volume_trend_percent < -20 THEN 'DECLINING'
        WHEN t.volume_trend_percent BETWEEN -20 AND 20 THEN 'STABLE'
        ELSE 'NEW_OR_DATA_INSUFFICIENT'
    END AS "Trend Direction",
    ROUND(t.volume_trend_percent, 2) AS "Volume Trend (%)"
    
FROM daily_top_merchants_by_count c
LEFT JOIN trend_analysis t ON c.merchant_id = t.merchant_id
WHERE c.daily_rank <= 20
ORDER BY c.transaction_date DESC, c.daily_rank;

-- 补充分析：新进入Top 20的商户识别
WITH 
daily_top_merchants_by_volume AS (
    SELECT 
        x.transaction_date,
        x.merchant_id,
        x.merchant_name,
        x.industry,
        x.daily_volume_usd,
        x.daily_success_count,
        x.daily_total_count,
        x.daily_volume_usd * 100.0 / NULLIF(SUM(x.daily_volume_usd) OVER(PARTITION BY x.transaction_date), 0) AS market_share_percent,
        RANK() OVER(PARTITION BY x.transaction_date ORDER BY x.daily_volume_usd DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) x
),
previous_day_top20 AS (
    SELECT merchant_id, daily_rank
    FROM daily_top_merchants_by_volume
    WHERE transaction_date = DATE_SUB(@analysis_date, INTERVAL 1 DAY)
      AND daily_rank <= 20
),
current_day_top20 AS (
    SELECT merchant_id, merchant_name, daily_rank, daily_volume_usd
    FROM daily_top_merchants_by_volume
    WHERE transaction_date = @analysis_date
      AND daily_rank <= 20
)
SELECT 
    'NEW_ENTRANTS' AS analysis_type,
    c.merchant_id AS "Merchant ID",
    c.merchant_name AS "Merchant Name",
    c.daily_rank AS "Current Rank",
    ROUND(c.daily_volume_usd, 2) AS "Current Volume (USD)",
    'NEW_IN_TOP20' AS "Status",
    '需要重点关注' AS "Alert Level"
FROM current_day_top20 c
LEFT JOIN previous_day_top20 p ON c.merchant_id = p.merchant_id
WHERE p.merchant_id IS NULL
ORDER BY c.daily_rank;

-- 连续多日霸榜商户分析
WITH 
daily_top_merchants_by_volume AS (
    SELECT 
        x.transaction_date,
        x.merchant_id,
        x.merchant_name,
        x.industry,
        x.daily_volume_usd,
        x.daily_success_count,
        x.daily_total_count,
        x.daily_volume_usd * 100.0 / NULLIF(SUM(x.daily_volume_usd) OVER(PARTITION BY x.transaction_date), 0) AS market_share_percent,
        RANK() OVER(PARTITION BY x.transaction_date ORDER BY x.daily_volume_usd DESC) AS daily_rank
    FROM (
        SELECT 
            t.transaction_date,
            t.merchant_id,
            m.merchant_name,
            m.industry,
            SUM(CASE WHEN t.status = 'success' THEN t.amount_usd ELSE 0 END) AS daily_volume_usd,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS daily_success_count,
            COUNT(*) AS daily_total_count
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.merchant_id
        WHERE t.transaction_date BETWEEN @start_date AND @analysis_date
        GROUP BY t.transaction_date, t.merchant_id, m.merchant_name, m.industry
    ) x
),
consecutive_ranking AS (
    SELECT 
    merchant_id,
    merchant_name,
        COUNT(DISTINCT transaction_date) AS days_in_top20,
        MIN(transaction_date) AS first_appearance,
        MAX(transaction_date) AS last_appearance,
        AVG(daily_volume_usd) AS avg_volume_usd,
        AVG(daily_rank) AS avg_rank
    FROM daily_top_merchants_by_volume
    WHERE transaction_date BETWEEN @start_date AND @analysis_date
      AND daily_rank <= 20
    GROUP BY merchant_id, merchant_name
    HAVING COUNT(DISTINCT transaction_date) >= 5  -- 至少5天在Top20
)
SELECT 
    'CONSISTENT_TOP20' AS analysis_type,
    merchant_id AS "Merchant ID",
    merchant_name AS "Merchant Name",
    days_in_top20 AS "Days in Top20 (7 days)",
    ROUND(avg_volume_usd, 2) AS "Avg Daily Volume (USD)",
    ROUND(avg_rank, 1) AS "Avg Rank",
    CASE 
        WHEN days_in_top20 = 7 THEN 'DOMINANT'
        WHEN days_in_top20 >= 5 THEN 'CONSISTENT'
        ELSE 'OCCASIONAL'
    END AS "Consistency Level",
    '稳定表现商户' AS "Analysis Note"
FROM consecutive_ranking
ORDER BY days_in_top20 DESC, avg_volume_usd DESC;