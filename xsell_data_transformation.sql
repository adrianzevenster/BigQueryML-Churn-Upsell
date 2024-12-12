
create or replace table  ensa.ensa_policy_summary as
with sub as (
    SELECT
    Customer_ID,
    Policy_Type,
    Policy_ID,
    start_date,
    renewal_date,
    tenure,
    SUM((case when (upper(Coverage) NOT like '%DESCONTO%' and coverage_code!=16) then 1 else 0 end) * premium_amount) as undiscounted_price,
    SUM((case when upper(Coverage) like '%DESCONTO%' then 1 else 0 end) * premium_amount) as applied_discounts,
    SUM((case when coverage_code=16 then 1 else 0 end) * premium_amount) as applied_bonus,
    count (distinct coverage_code) as scope_count,
    sum(premium_amount) as total_premium
    from `ensa-437407.ensa.ensa_policy_deduplicated`
    group by     Customer_ID,
    Policy_Type,
    Policy_ID,
    start_date,
    renewal_date,
    tenure
    ),
    sub2 as (
    select
      Customer_ID,
    Policy_Type,
    Policy_ID,
    scope_count,
    total_premium,
    undiscounted_price,
    applied_discounts,
    applied_bonus,
    ARRAY(
        SELECT AS STRUCT
            date,
            CAST(undiscounted_price AS BIGNUMERIC) / (case when (COUNT(date) OVER())=1 then 1 else ((COUNT(date) OVER())-1)end) as monthly_undiscounted,  -- Divide premium by the number of dates,
            CAST(applied_discounts AS BIGNUMERIC) / (case when (COUNT(date) OVER())=1 then 1 else ((COUNT(date) OVER())-1)end) as monthly_discount,  -- Divide premium by the number of dates,
            CAST(applied_bonus AS BIGNUMERIC) / (case when (COUNT(date) OVER())=1 then 1 else ((COUNT(date) OVER())-1)end) as monthly_bonus, -- Divide premium by the number of dates,
            CAST(total_premium AS BIGNUMERIC) / (case when (COUNT(date) OVER())=1 then 1 else ((COUNT(date) OVER())-1)end) as monthly_premium -- Divide premium by the number of dates
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(Start_Date, MONTH), DATE_TRUNC(DATE_SUB(Renewal_Date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 MONTH)) AS date
    ) AS daily_premium_array
    from sub
    )
    select
        Customer_ID,
        Policy_Type,
        Policy_ID,
        count(distinct policy_id) as policy_count,
        sum(scope_count) as coverage_count,
        daily.date,
        round(sum(daily.monthly_undiscounted)) as month_undiscounted,
        round(sum(daily.monthly_discount)) as month_discount,
        round(sum(daily.monthly_bonus)) as month_bonus,
        round(sum(daily.monthly_premium)) as month_premium
    from sub2, UNNEST(daily_premium_array) as daily
    group by customer_id,policy_id, policy_type, daily.date
    order by customer_id,policy_id,daily.date




create or replace table ensa.ensa_xsell_events as
    WITH MonthlySummary AS (
    SELECT
        customer_id,
        policy_type,
        date,
        COUNT(distinct policy_id) AS policy_count,
        SUM(coverage_count) AS coverage_count,
        SUM(month_undiscounted) AS total_undiscounted,
        SUM(month_discount) AS total_discount,
        SUM(month_bonus) AS total_bonus,
        SUM(month_premium) AS total_premium
    FROM
        `ensa-437407.ensa.ensa_policy_summary`
    GROUP BY
        customer_id,
        policy_type,
        date
),
RankedPolicies AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY policy_type, date) AS policy_rank
    FROM
        MonthlySummary
),
LaggedSummary AS (
    SELECT
        *,
        LAG(total_premium, 1, 0) OVER (PARTITION BY customer_id, policy_type ORDER BY date) AS previous_month_premium,
        LAG(coverage_count, 1, 0) OVER (PARTITION BY customer_id, policy_type ORDER BY date) AS previous_month_coverage_cnt,
        LAG(policy_count, 1, 0) OVER (PARTITION BY customer_id, policy_type ORDER BY date) AS previous_month_policy_cnt,
        LAG(policy_type, 1, NULL) OVER (PARTITION BY customer_id ORDER BY date) AS previous_policy_type

    FROM
        RankedPolicies
)
SELECT
    customer_id,
    policy_type,
    previous_policy_type,
    previous_month_premium,
    previous_month_coverage_cnt,
    previous_month_policy_cnt,
    date,
    policy_count,
    coverage_count,
    total_undiscounted,
    total_discount,
    total_bonus,
    total_premium,
    CASE
        WHEN ((total_premium >= previous_month_premium * 1.2 --Rule1: Upsell event if premium paid increased more than 20% for the same policy_type
            OR (coverage_count > previous_month_coverage_cnt and total_premium >= previous_month_premium) --Rule2: Upsell event if number of coverage areas have increased for the same policy_type.
            OR policy_count > previous_month_policy_cnt  ) --Rule3: Upsell event if number of policies have increased for the same policy_type.
            AND previous_month_premium + previous_month_coverage_cnt + previous_month_policy_cnt > 0)
        THEN 'Upsell'
        WHEN (policy_type != previous_policy_type  AND policy_rank = 1) THEN 'Cross-sell'  -- Rule4: If a customer purchased a policy of different policy_type. Mark only the first occurrence as Cross-sell
        ELSE NULL
    END AS event_type
FROM
    LaggedSummary
ORDER BY
    customer_id,
    date,
        policy_type
;

