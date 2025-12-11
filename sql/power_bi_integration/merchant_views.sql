USE test_database;
CREATE OR REPLACE VIEW output_suspicious_merchant AS
WITH merchant_first_tx AS (
    SELECT 
        `MERCHANT ID` AS merchant_id,
        MIN(`PAY TIME`) AS first_tx_time
    FROM tables
    GROUP BY `MERCHANT ID`
),
merchant_risk_calculation AS (
    SELECT 
        t.`MERCHANT ID` AS merchant_id,
        MAX(t.`MERCHANT NAME`) AS merchant_name,
        TIMESTAMPDIFF(DAY, mft.first_tx_time, CURRENT_DATE()) / 30 AS merchant_age_months,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS recent_30day_count,
        SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
        ELSE 0 END) AS recent_30day_volume_usd,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN 1 END) AS recent_180day_count,
        SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN 
            CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr2.`to_usd_rate` END 
        ELSE 0 END) AS recent_180day_volume_usd,
        (
            SELECT SUM(vol) FROM (
                SELECT tt.`USER ID`, SUM(CASE WHEN UPPER(tt.`STATUS`) = 'SUCCESS' AND DATE(tt.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 
                    CASE WHEN UPPER(tt.`PAY CURRENCY`) = 'USD' THEN tt.`PAY AMOUNT` ELSE tt.`PAY AMOUNT` * fr3.`to_usd_rate` END 
                ELSE 0 END) AS vol
                FROM tables tt
                LEFT JOIN fx_rates fr3 ON UPPER(fr3.`currency_code`) = UPPER(tt.`PAY CURRENCY`) AND fr3.`rate_date` = DATE(tt.`PAY TIME`)
                WHERE `MERCHANT ID` = t.`MERCHANT ID`
                GROUP BY tt.`USER ID`
                ORDER BY vol DESC
                LIMIT 10
            ) s
        ) / NULLIF(
            SUM(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 
                CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END 
            ELSE 0 END), 0
        ) * 100 AS top10_user_concentration_percent,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND 
                   (CASE WHEN UPPER(t.`PAY CURRENCY`) = 'USD' THEN t.`PAY AMOUNT` ELSE t.`PAY AMOUNT` * fr.`to_usd_rate` END) > (
                        SELECT AVG(CASE WHEN UPPER(t2.`PAY CURRENCY`) = 'USD' THEN t2.`PAY AMOUNT` ELSE t2.`PAY AMOUNT` * fr4.`to_usd_rate` END) * 3 
                        FROM tables t2 
                        LEFT JOIN fx_rates fr4 ON UPPER(fr4.`currency_code`) = UPPER(t2.`PAY CURRENCY`) AND fr4.`rate_date` = DATE(t2.`PAY TIME`)
                        WHERE t2.`MERCHANT ID` = t.`MERCHANT ID` AND UPPER(t2.`STATUS`) = 'SUCCESS' 
                              AND DATE(t2.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                   ) THEN 1 END) AS large_transaction_count_30day,
        COUNT(CASE WHEN UPPER(t.`STATUS`) = 'SUCCESS' AND DATE(t.`PAY TIME`) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND HOUR(t.`PAY TIME`) BETWEEN 0 AND 6 THEN 1 END) AS off_hours_transaction_count_30day
    FROM tables t
    LEFT JOIN merchant_first_tx mft ON mft.merchant_id = t.`MERCHANT ID`
    LEFT JOIN fx_rates fr ON UPPER(fr.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr.`rate_date` = DATE(t.`PAY TIME`)
    LEFT JOIN fx_rates fr2 ON UPPER(fr2.`currency_code`) = UPPER(t.`PAY CURRENCY`) AND fr2.`rate_date` = DATE(t.`PAY TIME`)
    GROUP BY t.`MERCHANT ID`
),
merchant_trend_analysis AS (
    SELECT 
        mrc.merchant_id,
        mrc.merchant_name,
        mrc.merchant_age_months,
        mrc.recent_30day_count,
        mrc.recent_30day_volume_usd,
        mrc.recent_180day_count,
        mrc.recent_180day_volume_usd,
        mrc.top10_user_concentration_percent,
        mrc.large_transaction_count_30day,
        mrc.off_hours_transaction_count_30day,
        CASE WHEN mrc.recent_180day_count > 0 
             THEN mrc.recent_180day_volume_usd / (mrc.recent_180day_count / 6)
             ELSE 0 
        END AS avg_30day_volume_usd,
        CASE WHEN mrc.recent_180day_count > 0 
             THEN mrc.recent_180day_count / 6 
             ELSE 0 
        END AS avg_30day_count,
        CASE WHEN mrc.recent_180day_count > 0 
             THEN (mrc.recent_30day_volume_usd - (mrc.recent_180day_volume_usd / 6)) * 100.0 / (mrc.recent_180day_volume_usd / 6)
             ELSE NULL 
        END AS growth_vs_180day_avg_percent
    FROM merchant_risk_calculation mrc
),
risk_scoring AS (
    SELECT 
        mta.*,
        (
            CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                 WHEN mta.top10_user_concentration_percent > 60 THEN 2
                 WHEN mta.top10_user_concentration_percent > 40 THEN 1
                 ELSE 0 END +
            CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                 WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                 WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                 ELSE 0 END +
            CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                 WHEN mta.large_transaction_count_30day > 5 THEN 2
                 WHEN mta.large_transaction_count_30day > 2 THEN 1
                 ELSE 0 END +
            CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                 WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                 ELSE 0 END +
            CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                 WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                 WHEN mta.merchant_age_months < 3 THEN 1
                 ELSE 0 END
        ) AS total_risk_score,
        CASE 
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END
            ) >= 15 THEN 'CRITICAL_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END
            ) >= 10 THEN 'HIGH_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END
            ) >= 6 THEN 'MEDIUM_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END
            ) >= 3 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS risk_classification,
        CONCAT(
            CASE WHEN mta.top10_user_concentration_percent > 80 THEN '用户高度集中; ' ELSE '' END,
            CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN '交易量异常波动; ' ELSE '' END,
            CASE WHEN mta.large_transaction_count_30day > 10 THEN '大额交易频繁; ' ELSE '' END,
            CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN '异常时间交易; ' ELSE '' END,
            CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN '新商户大额交易; ' ELSE '' END
        ) AS primary_risk_factors
    FROM merchant_trend_analysis mta
)
SELECT 
    merchant_id AS "Merchant ID",
    merchant_name AS "Merchant Name",
    ROUND(merchant_age_months, 1) AS "Merchant Age (Months)",
    recent_30day_count AS "Recent 30-Day Count",
    ROUND(recent_30day_volume_usd, 2) AS "Recent 30-Day Volume (USD)",
    ROUND(recent_30day_volume_usd / NULLIF(recent_30day_count, 0), 2) AS "Recent 30-Day Avg Amount (USD)",
    ROUND(top10_user_concentration_percent, 2) AS "Top 10 User Concentration (%)",
    large_transaction_count_30day AS "Large Transaction Count (30-Day)",
    off_hours_transaction_count_30day AS "Off-Hours Transaction Count (30-Day)",
    ROUND(off_hours_transaction_count_30day * 100.0 / NULLIF(recent_30day_count, 0), 2) AS "Off-Hours Transaction Ratio (%)",
    ROUND(avg_30day_volume_usd, 2) AS "180-Day Avg 30-Day Volume (USD)",
    ROUND(avg_30day_count, 0) AS "180-Day Avg 30-Day Count",
    ROUND(growth_vs_180day_avg_percent, 2) AS "Growth vs 180-Day Avg (%)",
    total_risk_score AS "Total Risk Score",
    risk_classification AS "Risk Classification",
    primary_risk_factors AS "Primary Risk Factors",
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '🔴 CRITICAL'
        WHEN risk_classification = 'HIGH_RISK' THEN '🔴 HIGH'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '🟡 MEDIUM'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '🟢 LOW-MEDIUM'
        ELSE '🟢 LOW'
    END AS "Risk Level Display",
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '立即冻结账户，启动紧急调查程序'
        WHEN risk_classification = 'HIGH_RISK' THEN '加强监控，要求提供业务说明，考虑限制措施'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '增加监控频率，定期评估风险状况'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '保持正常监控，关注指标变化'
        ELSE '保持标准监控程序'
    END AS "Recommended Actions",
    CURRENT_TIMESTAMP() AS "Last Updated"
FROM risk_scoring
WHERE total_risk_score > 0
ORDER BY total_risk_score DESC, recent_30day_volume_usd DESC;
-- 目标：为Power BI提供商户风险分析和趋势数据

-- 创建商户风险标记视图
CREATE OR REPLACE VIEW output_suspicious_merchant AS
WITH merchant_risk_calculation AS (
    SELECT 
        m.merchant_id,
        m.merchant_name,
        m.industry,
        m.risk_level AS original_risk_level,
        m.join_date,
        m.business_type,
        m.registered_country,
        
        -- 计算商户年龄（月）
        DATEDIFF(CURRENT_DATE(), m.join_date) / 30 AS merchant_age_months,
        
        -- 最近30天表现
        COUNT(CASE WHEN t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS recent_30day_count,
        SUM(CASE WHEN t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END) AS recent_30day_volume_usd,
        
        -- 最近180天表现
        COUNT(CASE WHEN t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN 1 END) AS recent_180day_count,
        SUM(CASE WHEN t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN t.amount_usd ELSE 0 END) AS recent_180day_volume_usd,
        
        -- 用户集中度（Top 10用户占比）
        (SELECT SUM(amount_usd) FROM (
            SELECT user_id, SUM(amount_usd) AS amount_usd
            FROM transactions 
            WHERE merchant_id = m.merchant_id AND status = 'success' 
                  AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY user_id
            ORDER BY SUM(amount_usd) DESC
            LIMIT 10
        ) top_users) / NULLIF(
            SUM(CASE WHEN t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END), 0
        ) * 100 AS top10_user_concentration_percent,
        
        -- Payin/Payout分析
        COUNT(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS recent_30day_payin_count,
        COUNT(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS recent_30day_payout_count,
        SUM(CASE WHEN t.transaction_type = 'payin' AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END) AS recent_30day_payin_volume_usd,
        SUM(CASE WHEN t.transaction_type = 'payout' AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END) AS recent_30day_payout_volume_usd,
        
        -- 大额交易分析（>平均3倍）
        COUNT(CASE WHEN t.amount_usd > (
            SELECT AVG(amount_usd) * 3 
            FROM transactions t2 
            WHERE t2.merchant_id = m.merchant_id AND t2.status = 'success' 
                  AND t2.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ) AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS large_transaction_count_30day,
        
        -- 异常时间交易（非工作时间）
        COUNT(CASE WHEN HOUR(t.transaction_time) BETWEEN 0 AND 6 AND t.status = 'success' AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS off_hours_transaction_count_30day
        
    FROM merchants m
    LEFT JOIN transactions t ON m.merchant_id = t.merchant_id
    WHERE m.status = 'active'
    GROUP BY m.merchant_id, m.merchant_name, m.industry, m.risk_level, m.join_date, m.business_type, m.registered_country
),

-- 计算180天移动平均和趋势
merchant_trend_analysis AS (
    SELECT 
        mrc.merchant_id,
        mrc.merchant_name,
        mrc.industry,
        mrc.original_risk_level,
        mrc.join_date,
        mrc.business_type,
        mrc.registered_country,
        mrc.merchant_age_months,
        mrc.recent_30day_count,
        mrc.recent_30day_volume_usd,
        mrc.recent_180day_count,
        mrc.recent_180day_volume_usd,
        mrc.top10_user_concentration_percent,
        mrc.recent_30day_payin_count,
        mrc.recent_30day_payout_count,
        mrc.recent_30day_payin_volume_usd,
        mrc.recent_30day_payout_volume_usd,
        mrc.large_transaction_count_30day,
        mrc.off_hours_transaction_count_30day,
        
        -- 计算180天日均值
        CASE WHEN mrc.recent_180day_count > 0 
             THEN mrc.recent_180day_volume_usd / (mrc.recent_180day_count / 6)  -- 转换为30天平均
             ELSE 0 
        END AS avg_30day_volume_usd,
        
        -- 计算180天日均交易数
        CASE WHEN mrc.recent_180day_count > 0 
             THEN mrc.recent_180day_count / 6  -- 转换为30天平均
             ELSE 0 
        END AS avg_30day_count,
        
        -- 与180天平均对比的增长率
        CASE WHEN mrc.recent_180day_count > 0 
             THEN (mrc.recent_30day_volume_usd - (mrc.recent_180day_volume_usd / 6)) * 100.0 / (mrc.recent_180day_volume_usd / 6)
             ELSE NULL 
        END AS growth_vs_180day_avg_percent,
        
        -- Payin/Payout不平衡度
        CASE WHEN mrc.recent_30day_payin_volume_usd + mrc.recent_30day_payout_volume_usd > 0
             THEN ABS(mrc.recent_30day_payin_volume_usd - mrc.recent_30day_payout_volume_usd) * 100.0 / 
                  (mrc.recent_30day_payin_volume_usd + mrc.recent_30day_payout_volume_usd)
             ELSE 0
        END AS payin_payout_imbalance_percent
        
    FROM merchant_risk_calculation mrc
),

-- 风险评分和标记
risk_scoring AS (
    SELECT 
        mta.*,
        
        -- 计算综合风险评分
        (
            -- 集中度风险（Top 10用户占比>80%）
            CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                 WHEN mta.top10_user_concentration_percent > 60 THEN 2
                 WHEN mta.top10_user_concentration_percent > 40 THEN 1
                 ELSE 0 END +
            
            -- 波动性风险（与180天平均差异>±15%）
            CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                 WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                 WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                 ELSE 0 END +
            
            -- Payin/Payout不平衡风险
            CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 3
                 WHEN mta.payin_payout_imbalance_percent > 60 THEN 2
                 WHEN mta.payin_payout_imbalance_percent > 40 THEN 1
                 ELSE 0 END +
            
            -- 大额交易风险
            CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                 WHEN mta.large_transaction_count_30day > 5 THEN 2
                 WHEN mta.large_transaction_count_30day > 2 THEN 1
                 ELSE 0 END +
            
            -- 异常时间交易风险
            CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                 WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                 ELSE 0 END +
            
            -- 新商户风险（注册<3个月）
            CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                 WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                 WHEN mta.merchant_age_months < 3 THEN 1
                 ELSE 0 END +
            
            -- 原始风险等级
            CASE WHEN mta.original_risk_level = 'high' THEN 2
                 WHEN mta.original_risk_level = 'medium' THEN 1
                 ELSE 0 END
        ) AS total_risk_score,
        
        -- 风险等级分类
        CASE 
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 3
                     WHEN mta.payin_payout_imbalance_percent > 60 THEN 2
                     WHEN mta.payin_payout_imbalance_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.original_risk_level = 'high' THEN 2
                     WHEN mta.original_risk_level = 'medium' THEN 1
                     ELSE 0 END
            ) >= 15 THEN 'CRITICAL_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 3
                     WHEN mta.payin_payout_imbalance_percent > 60 THEN 2
                     WHEN mta.payin_payout_imbalance_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.original_risk_level = 'high' THEN 2
                     WHEN mta.original_risk_level = 'medium' THEN 1
                     ELSE 0 END
            ) >= 10 THEN 'HIGH_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 3
                     WHEN mta.payin_payout_imbalance_percent > 60 THEN 2
                     WHEN mta.payin_payout_imbalance_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.original_risk_level = 'high' THEN 2
                     WHEN mta.original_risk_level = 'medium' THEN 1
                     ELSE 0 END
            ) >= 6 THEN 'MEDIUM_RISK'
            WHEN (
                CASE WHEN mta.top10_user_concentration_percent > 80 THEN 4
                     WHEN mta.top10_user_concentration_percent > 60 THEN 2
                     WHEN mta.top10_user_concentration_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN 4
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 30 THEN 3
                     WHEN ABS(mta.growth_vs_180day_avg_percent) > 15 THEN 2
                     ELSE 0 END +
                CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 3
                     WHEN mta.payin_payout_imbalance_percent > 60 THEN 2
                     WHEN mta.payin_payout_imbalance_percent > 40 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.large_transaction_count_30day > 10 THEN 3
                     WHEN mta.large_transaction_count_30day > 5 THEN 2
                     WHEN mta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN 2
                     WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.15 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 100000 THEN 3
                     WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN 2
                     WHEN mta.merchant_age_months < 3 THEN 1
                     ELSE 0 END +
                CASE WHEN mta.original_risk_level = 'high' THEN 2
                     WHEN mta.original_risk_level = 'medium' THEN 1
                     ELSE 0 END
            ) >= 3 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS risk_classification,
        
        -- 主要风险因素
        CONCAT(
            CASE WHEN mta.top10_user_concentration_percent > 80 THEN '用户高度集中; ' ELSE '' END,
            CASE WHEN ABS(mta.growth_vs_180day_avg_percent) > 50 THEN '交易量异常波动; ' ELSE '' END,
            CASE WHEN mta.payin_payout_imbalance_percent > 80 THEN 'Payin/Payout严重不平衡; ' ELSE '' END,
            CASE WHEN mta.large_transaction_count_30day > 10 THEN '大额交易频繁; ' ELSE '' END,
            CASE WHEN mta.off_hours_transaction_count_30day > mta.recent_30day_count * 0.3 THEN '异常时间交易; ' ELSE '' END,
            CASE WHEN mta.merchant_age_months < 3 AND mta.recent_30day_volume_usd > 50000 THEN '新商户大额交易; ' ELSE '' END,
            CASE WHEN mta.original_risk_level IN ('high', 'medium') THEN CONCAT('原始风险等级:', mta.original_risk_level, '; ') ELSE '' END
        ) AS primary_risk_factors
        
    FROM merchant_trend_analysis mta
)

-- 最终输出：商户风险视图
SELECT 
    merchant_id AS "Merchant ID",
    merchant_name AS "Merchant Name",
    industry AS "Industry",
    original_risk_level AS "Original Risk Level",
    join_date AS "Join Date",
    business_type AS "Business Type",
    registered_country AS "Registered Country",
    ROUND(merchant_age_months, 1) AS "Merchant Age (Months)",
    
    -- 交易指标
    recent_30day_count AS "Recent 30-Day Count",
    ROUND(recent_30day_volume_usd, 2) AS "Recent 30-Day Volume (USD)",
    ROUND(recent_30day_volume_usd / NULLIF(recent_30day_count, 0), 2) AS "Recent 30-Day Avg Amount (USD)",
    
    -- 集中度指标
    ROUND(top10_user_concentration_percent, 2) AS "Top 10 User Concentration (%)",
    
    -- Payin/Payout分析
    recent_30day_payin_count AS "Recent 30-Day Payin Count",
    recent_30day_payout_count AS "Recent 30-Day Payout Count",
    ROUND(recent_30day_payin_volume_usd, 2) AS "Recent 30-Day Payin Volume (USD)",
    ROUND(recent_30day_payout_volume_usd, 2) AS "Recent 30-Day Payout Volume (USD)",
    ROUND(payin_payout_imbalance_percent, 2) AS "Payin/Payout Imbalance (%)",
    
    -- 异常指标
    large_transaction_count_30day AS "Large Transaction Count (30-Day)",
    off_hours_transaction_count_30day AS "Off-Hours Transaction Count (30-Day)",
    ROUND(off_hours_transaction_count_30day * 100.0 / NULLIF(recent_30day_count, 0), 2) AS "Off-Hours Transaction Ratio (%)",
    
    -- 趋势分析
    ROUND(avg_30day_volume_usd, 2) AS "180-Day Avg 30-Day Volume (USD)",
    ROUND(avg_30day_count, 0) AS "180-Day Avg 30-Day Count",
    ROUND(growth_vs_180day_avg_percent, 2) AS "Growth vs 180-Day Avg (%)",
    
    -- 风险评分
    total_risk_score AS "Total Risk Score",
    risk_classification AS "Risk Classification",
    primary_risk_factors AS "Primary Risk Factors",
    
    -- 风险等级图标
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '🔴 CRITICAL'
        WHEN risk_classification = 'HIGH_RISK' THEN '🔴 HIGH'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '🟡 MEDIUM'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '🟢 LOW-MEDIUM'
        ELSE '🟢 LOW'
    END AS "Risk Level Display",
    
    -- 建议措施
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '立即冻结账户，启动紧急调查程序'
        WHEN risk_classification = 'HIGH_RISK' THEN '加强监控，要求提供业务说明，考虑限制措施'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '增加监控频率，定期评估风险状况'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '保持正常监控，关注指标变化'
        ELSE '保持标准监控程序'
    END AS "Recommended Actions",
    
    -- 时间戳
    CURRENT_TIMESTAMP() AS "Last Updated"
    
FROM risk_scoring
WHERE total_risk_score > 0  -- 只显示有风险的商户
ORDER BY total_risk_score DESC, recent_30day_volume_usd DESC;