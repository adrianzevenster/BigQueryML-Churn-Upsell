SELECT
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium,
      `date`,
       DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,
      predicted_event_type,
      (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Do Nothing') AS do_nothing_prob,
      (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Upsell') AS upsell_prob,
      (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Cross-sell') AS cross_sell_prob,
       CASE
          WHEN  predicted_event_type = 'Upsell' then (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Upsell')
          WHEN predicted_event_type = 'Cross-sell' then (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Cross-sell')
          ELSE (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Do Nothing')
        END
        AS probability
FROM ML.PREDICT(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`,
  (
SELECT
  CASE
    WHEN event_type IS NULL THEN 'Do Nothing'  -- Map NULL to "Do Nothing"
    WHEN DATE_DIFF(`date`, CURRENT_DATE(), DAY) < 0 THEN
      CASE
        WHEN event_type = 'Do Nothing' THEN 'Cross-sell'  -- Force "Cross-sell" if negative
        ELSE event_type
      END
    ELSE event_type  -- Keep the original event_type if Days_to_Renewal is non-negative
  END AS event_type,  -- Final event_type with rules applied
  DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,  -- Include the new feature
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  coverage_count,
  total_premium
FROM `ensa-437407.ensa.ensa_xsell_events`
  )
)
;