select *, 'Logistic Regression' AS ModelType
from ml.evaluate(
  model `ensa.xsell_lg_model_renewal_day`,
  (
    SELECT *, DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal
    FROM `ensa.ensa_xsell_events_training_data_stratified_do_null`
    WHERE data_split = 'TEST'
  )
)

UNION ALL

SELECT *, 'Random Forest Classifier' AS ModelType
from ml.evaluate(
  model `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`,
  (
    select
      case
        when event_type is null then 'Do Nothing'
        else event_type
      end as event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium,
      DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal
    from `ensa.ensa_xsell_events_training_data_stratified_do_null`
    where data_split = 'TEST'
  )
)

UNION ALL

select *, 'Gradient Boost' AS ModelType
from ml.evaluate(
  model `ensa.xsell_gboost_model`,
  (
    SELECT *
    FROM `ensa.ensa_xsell_events_training_data_stratified_do_null`
    where data_split = 'TEST'
  )
)

UNION ALL

SELECT * , 'Deep Neural Network' AS ModelType
FROM ML.EVALUATE(
  MODEL `ensa.xsell_dnn_model`,
  (
    SELECT
      DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium,
      event_type
    FROM `ensa-437407.ensa.ensa_xsell_events_training_data_stratified_do_null`
    WHERE data_split = 'TEST'
  )
)
;