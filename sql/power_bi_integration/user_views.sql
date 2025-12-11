USE test_database;
CREATE OR REPLACE VIEW output_suspicious_user AS
WITH user_risk_calculation AS (
    SELECT 
        u.user_id,
        MAX(u.user_name) AS user_name,
        NULL AS email,
        NULL AS phone,
        DATEDIFF(CURRENT_DATE(), MIN(u.registration_date)) / 30 AS user_age_months,
        COUNT(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS recent_30day_count,
        SUM(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END) AS recent_30day_volume_usd,
        COUNT(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN 1 END) AS recent_180day_count,
        SUM(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN t.amount_usd ELSE 0 END) AS recent_180day_volume_usd,
        (
            SELECT SUM(amount_usd) FROM (
                SELECT merchant_id, SUM(amount_usd) AS amount_usd
                FROM transactions tt
                WHERE tt.user_id = u.user_id AND tt.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
                      AND tt.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY merchant_id
                ORDER BY SUM(amount_usd) DESC
                LIMIT 5
            ) s
        ) / NULLIF(
            SUM(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN t.amount_usd ELSE 0 END), 0
        ) * 100 AS top5_merchant_concentration_percent,
        COUNT(CASE WHEN t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND 
                   t.amount_usd > (
                        SELECT AVG(t2.amount_usd) * 3 
                        FROM transactions t2 
                        WHERE t2.user_id = u.user_id AND t2.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
                              AND t2.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                   ) THEN 1 END) AS large_transaction_count_30day,
        COUNT(CASE WHEN ((t.amount_usd BETWEEN 4990 AND 5010) OR (t.amount_usd BETWEEN 9990 AND 10010)) 
                    AND t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS sensitive_amount_count_30day,
        COUNT(CASE WHEN HOUR(t.transaction_time) BETWEEN 0 AND 6 AND t.status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID') AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) AS off_hours_transaction_count_30day,
        (
            SELECT COUNT(*) FROM (
                SELECT DATE(transaction_time) AS tx_date, COUNT(*) AS daily_count
                FROM transactions 
                WHERE user_id = u.user_id AND status IN ('SUCCESS','SUCCEEDED','APPROVAL','AUTORIZADO','PAID')
                      AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY DATE(transaction_time)
                HAVING COUNT(*) > 10
            ) high_frequency_days
        ) AS high_frequency_days_count_30day
    FROM users u
    LEFT JOIN transactions t ON t.user_id = u.user_id
    WHERE u.status = 'active'
    GROUP BY u.user_id
),
user_trend_analysis AS (
    SELECT 
        urc.user_id,
        urc.user_name,
        urc.email,
        urc.phone,
        urc.user_age_months,
        urc.recent_30day_count,
        urc.recent_30day_volume_usd,
        urc.recent_180day_count,
        urc.recent_180day_volume_usd,
        urc.top5_merchant_concentration_percent,
        urc.large_transaction_count_30day,
        urc.sensitive_amount_count_30day,
        urc.off_hours_transaction_count_30day,
        urc.high_frequency_days_count_30day,
        CASE WHEN urc.recent_180day_count > 0 
             THEN urc.recent_180day_volume_usd / (urc.recent_180day_count / 6)
             ELSE 0 
        END AS avg_30day_volume_usd,
        CASE WHEN urc.recent_180day_count > 0 
             THEN urc.recent_180day_count / 6 
             ELSE 0 
        END AS avg_30day_count,
        CASE WHEN urc.recent_180day_count > 0 
             THEN (urc.recent_30day_volume_usd - (urc.recent_180day_volume_usd / 6)) * 100.0 / (urc.recent_180day_volume_usd / 6)
             ELSE NULL 
        END AS growth_vs_180day_avg_percent
    FROM user_risk_calculation urc
),
risk_scoring AS (
    SELECT 
        uta.*,
        (
            CASE WHEN uta.top5_merchant_concentration_percent > 90 THEN 4
                 WHEN uta.top5_merchant_concentration_percent > 80 THEN 3
                 WHEN uta.top5_merchant_concentration_percent > 70 THEN 2
                 WHEN uta.top5_merchant_concentration_percent > 50 THEN 1
                 ELSE 0 END +
            CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 100 THEN 4
                 WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN 3
                 WHEN ABS(uta.growth_vs_180day_avg_percent) > 30 THEN 2
                 WHEN ABS(uta.growth_vs_180day_avg_percent) > 15 THEN 1
                 ELSE 0 END +
            CASE WHEN uta.large_transaction_count_30day > 10 THEN 3
                 WHEN uta.large_transaction_count_30day > 5 THEN 2
                 WHEN uta.large_transaction_count_30day > 2 THEN 1
                 ELSE 0 END +
            CASE WHEN uta.sensitive_amount_count_30day > 5 THEN 3
                 WHEN uta.sensitive_amount_count_30day > 3 THEN 2
                 WHEN uta.sensitive_amount_count_30day > 1 THEN 1
                 ELSE 0 END +
            CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.4 THEN 3
                 WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN 2
                 WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.1 THEN 1
                 ELSE 0 END +
            CASE WHEN uta.high_frequency_days_count_30day > 5 THEN 3
                 WHEN uta.high_frequency_days_count_30day > 3 THEN 2
                 WHEN uta.high_frequency_days_count_30day > 1 THEN 1
                 ELSE 0 END +
            CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 10000 THEN 3
                 WHEN uta.user_age_months < 1 THEN 1
                 ELSE 0 END
        ) AS total_risk_score,
        CASE 
            WHEN (
                CASE WHEN uta.top5_merchant_concentration_percent > 90 THEN 4
                     WHEN uta.top5_merchant_concentration_percent > 80 THEN 3
                     WHEN uta.top5_merchant_concentration_percent > 70 THEN 2
                     WHEN uta.top5_merchant_concentration_percent > 50 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 100 THEN 4
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN 3
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 30 THEN 2
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 15 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.large_transaction_count_30day > 10 THEN 3
                     WHEN uta.large_transaction_count_30day > 5 THEN 2
                     WHEN uta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.sensitive_amount_count_30day > 5 THEN 3
                     WHEN uta.sensitive_amount_count_30day > 3 THEN 2
                     WHEN uta.sensitive_amount_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.4 THEN 3
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN 2
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.high_frequency_days_count_30day > 5 THEN 3
                     WHEN uta.high_frequency_days_count_30day > 3 THEN 2
                     WHEN uta.high_frequency_days_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 10000 THEN 3
                     WHEN uta.user_age_months < 1 THEN 1
                     ELSE 0 END
            ) >= 20 THEN 'CRITICAL_RISK'
            WHEN (
                CASE WHEN uta.top5_merchant_concentration_percent > 90 THEN 4
                     WHEN uta.top5_merchant_concentration_percent > 80 THEN 3
                     WHEN uta.top5_merchant_concentration_percent > 70 THEN 2
                     WHEN uta.top5_merchant_concentration_percent > 50 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 100 THEN 4
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN 3
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 30 THEN 2
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 15 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.large_transaction_count_30day > 10 THEN 3
                     WHEN uta.large_transaction_count_30day > 5 THEN 2
                     WHEN uta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.sensitive_amount_count_30day > 5 THEN 3
                     WHEN uta.sensitive_amount_count_30day > 3 THEN 2
                     WHEN uta.sensitive_amount_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.4 THEN 3
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN 2
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.high_frequency_days_count_30day > 5 THEN 3
                     WHEN uta.high_frequency_days_count_30day > 3 THEN 2
                     WHEN uta.high_frequency_days_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 10000 THEN 3
                     WHEN uta.user_age_months < 1 THEN 1
                     ELSE 0 END
            ) >= 12 THEN 'HIGH_RISK'
            WHEN (
                CASE WHEN uta.top5_merchant_concentration_percent > 90 THEN 4
                     WHEN uta.top5_merchant_concentration_percent > 80 THEN 3
                     WHEN uta.top5_merchant_concentration_percent > 70 THEN 2
                     WHEN uta.top5_merchant_concentration_percent > 50 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 100 THEN 4
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN 3
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 30 THEN 2
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 15 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.large_transaction_count_30day > 10 THEN 3
                     WHEN uta.large_transaction_count_30day > 5 THEN 2
                     WHEN uta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.sensitive_amount_count_30day > 5 THEN 3
                     WHEN uta.sensitive_amount_count_30day > 3 THEN 2
                     WHEN uta.sensitive_amount_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.4 THEN 3
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN 2
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.high_frequency_days_count_30day > 5 THEN 3
                     WHEN uta.high_frequency_days_count_30day > 3 THEN 2
                     WHEN uta.high_frequency_days_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 10000 THEN 3
                     WHEN uta.user_age_months < 1 THEN 1
                     ELSE 0 END
            ) >= 6 THEN 'MEDIUM_RISK'
            WHEN (
                CASE WHEN uta.top5_merchant_concentration_percent > 90 THEN 4
                     WHEN uta.top5_merchant_concentration_percent > 80 THEN 3
                     WHEN uta.top5_merchant_concentration_percent > 70 THEN 2
                     WHEN uta.top5_merchant_concentration_percent > 50 THEN 1
                     ELSE 0 END +
                CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 100 THEN 4
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN 3
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 30 THEN 2
                     WHEN ABS(uta.growth_vs_180day_avg_percent) > 15 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.large_transaction_count_30day > 10 THEN 3
                     WHEN uta.large_transaction_count_30day > 5 THEN 2
                     WHEN uta.large_transaction_count_30day > 2 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.sensitive_amount_count_30day > 5 THEN 3
                     WHEN uta.sensitive_amount_count_30day > 3 THEN 2
                     WHEN uta.sensitive_amount_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.4 THEN 3
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN 2
                     WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.high_frequency_days_count_30day > 5 THEN 3
                     WHEN uta.high_frequency_days_count_30day > 3 THEN 2
                     WHEN uta.high_frequency_days_count_30day > 1 THEN 1
                     ELSE 0 END +
                CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 10000 THEN 3
                     WHEN uta.user_age_months < 1 THEN 1
                     ELSE 0 END
            ) >= 3 THEN 'LOW_MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END AS risk_classification,
        CONCAT(
            CASE WHEN uta.top5_merchant_concentration_percent > 80 THEN '商户高度集中; ' ELSE '' END,
            CASE WHEN ABS(uta.growth_vs_180day_avg_percent) > 50 THEN '交易量异常波动; ' ELSE '' END,
            CASE WHEN uta.large_transaction_count_30day > 5 THEN '大额交易频繁; ' ELSE '' END,
            CASE WHEN uta.sensitive_amount_count_30day > 3 THEN '敏感金额模式; ' ELSE '' END,
            CASE WHEN uta.off_hours_transaction_count_30day > uta.recent_30day_count * 0.2 THEN '非工作时间交易; ' ELSE '' END,
            CASE WHEN uta.high_frequency_days_count_30day > 3 THEN '高频交易日; ' ELSE '' END,
            CASE WHEN uta.user_age_months < 1 AND uta.recent_30day_volume_usd > 5000 THEN '新用户大额交易; ' ELSE '' END
        ) AS primary_risk_factors
    FROM user_trend_analysis uta
)
SELECT 
    user_id AS "User ID",
    user_name AS "User Name",
    email AS "Email",
    phone AS "Phone",
    ROUND(user_age_months, 1) AS "User Age (Months)",
    recent_30day_count AS "Recent 30-Day Count",
    ROUND(recent_30day_volume_usd, 2) AS "Recent 30-Day Volume (USD)",
    ROUND(recent_30day_volume_usd / NULLIF(recent_30day_count, 0), 2) AS "Recent 30-Day Avg Amount (USD)",
    recent_180day_count AS "Recent 180-Day Count",
    ROUND(recent_180day_volume_usd, 2) AS "Recent 180-Day Volume (USD)",
    ROUND(avg_30day_volume_usd, 2) AS "180-Day Avg 30-Day Volume (USD)",
    ROUND(avg_30day_count, 0) AS "180-Day Avg 30-Day Count",
    ROUND(top5_merchant_concentration_percent, 2) AS "Top 5 Merchant Concentration (%)",
    large_transaction_count_30day AS "Large Transaction Count (30-Day)",
    sensitive_amount_count_30day AS "Sensitive Amount Count (30-Day)",
    off_hours_transaction_count_30day AS "Off-Hours Transaction Count (30-Day)",
    ROUND(off_hours_transaction_count_30day * 100.0 / NULLIF(recent_30day_count, 0), 2) AS "Off-Hours Transaction Ratio (%)",
    high_frequency_days_count_30day AS "High Frequency Days Count (30-Day)",
    ROUND(growth_vs_180day_avg_percent, 2) AS "Growth vs 180-Day Avg (%)",
    total_risk_score AS "Total Risk Score",
    risk_classification AS "Risk Classification",
    primary_risk_factors AS "Primary Risk Factors",
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '[CRITICAL]'
        WHEN risk_classification = 'HIGH_RISK' THEN '[HIGH]'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '[MEDIUM]'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '[LOW-MEDIUM]'
        ELSE '[LOW]'
    END AS "Risk Level Display",
    CASE 
        WHEN risk_classification = 'CRITICAL_RISK' THEN '立即冻结账户，启动紧急调查程序，联系用户核实身份'
        WHEN risk_classification = 'HIGH_RISK' THEN '加强监控，要求提供资金来源证明，限制大额交易，进行增强尽职调查'
        WHEN risk_classification = 'MEDIUM_RISK' THEN '增加监控频率，定期要求用户更新信息，分析交易模式变化'
        WHEN risk_classification = 'LOW_MEDIUM_RISK' THEN '保持正常监控，关注风险指标变化，定期回顾用户风险状况'
        ELSE '保持标准监控程序，定期例行检查'
    END AS "Recommended Actions",
    CURRENT_TIMESTAMP() AS "Last Updated"
FROM risk_scoring
WHERE total_risk_score > 0
ORDER BY total_risk_score DESC, recent_30day_volume_usd DESC;
