select * from ensa.ensa_xsell_events
where event_type in ('Upsell','Cross-sell')
order by customer_id, date,policy_type;

-- Splitting the dataset into training and testing (80-20 split)
CREATE OR REPLACE TABLE ensa-437407.ensa.ensa_xsell_events_training_data AS
SELECT *,
  CASE
    WHEN RAND() < 0.8 THEN 'TRAIN'
    ELSE 'TEST'
  END AS data_split
FROM ensa.ensa_xsell_events

-- Stratified Training / Testing
CREATE OR REPLACE TABLE `ensa-437407.ensa.ensa_xsell_events_training_data_stratified_do_null` AS
SELECT *,
  CASE
    WHEN RAND() < 0.8 THEN 'TRAIN'
    ELSE 'TEST'
  END AS data_split
FROM (
  SELECT *, RAND() AS random_value
  FROM `ensa.ensa_xsell_events`
  --WHERE event_type IS NOT NULL -- Ensure no NULLs in the target column
)
QUALIFY NTILE(2) OVER (PARTITION BY event_type ORDER BY random_value) = 1;

-- Improved RF Model with Stratified Sample
create or replace model `ensa-437407.ensa.xsell_lg_model_stratified`
options (
  model_type='logistic_reg',
  input_label_cols=['event_type'],
  max_iterations=50
) as
select
  event_type,
  policy_count,
  coverage_count,
  previous_month_premium,
  previous_month_premium / policy_count as avg_policy_premium,
  policy_count * coverage_count as policy_coverage_interaction,
  total_premium,
  previous_policy_type,
  policy_type,
  previous_month_policy_cnt
from `ensa.ensa_xsell_events_training_data_stratified`
where data_split = 'TRAIN';

-- Evaluating updated lg model on stratified data
select *
from ml.evaluate(
  model ensa.xsell_lg_model_stratified,
  (
  select
  event_type,
  policy_count,
  coverage_count,
  previous_month_premium,
  previous_month_premium / policy_count as avg_policy_premium,
  policy_count * coverage_count as policy_coverage_interaction,
  total_premium,
  previous_policy_type,
  policy_type,
  previous_month_policy_cnt
from `ensa.ensa_xsell_events_training_data_stratified`
where data_split = 'TEST'
)
);


CREATE OR REPLACE MODEL `ensa.xsell_lg_model`
OPTIONS(
    model_type='logistic_reg',
    input_label_cols=['event_type']
) AS
SELECT
  event_type,
  customer_id,
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  coverage_count,
  total_premium
FROM `ensa-437407.ensa.ensa_xsell_events_training_data`
WHERE data_split = 'TRAIN'
AND event_type IS NOT NULL;



CREATE OR REPLACE TABLE `ensa-437407.ensa.xsell_lg_predictions` AS
SELECT *
FROM ml.predict(
  MODEL `ensa.xsell_lg_model`,
  (
    SELECT
      event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM `ensa.ensa_xsell_events`
  )
);

-- Evaluate Lg model for predicting upsell and Cross-sell
select *
from ml.evaluate(
  model `ensa.xsell_lg_model`,
  (
    SELECT *
    FROM `ensa-437407.ensa.ensa_xsell_events_training_data`
    WHERE data_split = 'TEST'
    AND event_type IS NOT NULL
  )
);

select *
from ml.predict(
  model `ensa.xsell_lg_model`,
  (
    SELECT
      event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM ensa.ensa_xsell_events
  )
);

CREATE OR REPLACE TABLE `ensa-437407.ensa.xsell_lg_predictions` AS
SELECT *
FROM ml.predict(
  MODEL `ensa.xsell_lg_model`,
  (
    SELECT
      event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM `ensa.ensa_xsell_events`
  )
);

create or replace model `ensa.xsell_gboost_model`
OPTIONS(
  model_type='boosted_tree_classifier',
  input_label_cols=['event_type'],
  max_iterations=100,
  tree_method='auto',
  learn_rate=0.1,
  subsample=0.8
) AS
SELECT
  event_type,
  customer_id,
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  total_premium
FROM `ensa-437407.ensa.ensa_xsell_events_training_data`
WHERE data_split = 'TRAIN'
AND event_type is not null;

select *
from ml.evaluate(
  model `ensa.xsell_gboost_model`,
  (
    SELECT *
    FROM `ensa-437407.ensa.ensa_xsell_events_training_data`
    where data_split = 'TEST'
    and event_type is not null
  )
);
select *
from ml.predict(
  model `ensa.xsell_lg_model`,
  (
    SELECT
      event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM ensa.ensa_xsell_events
  )
);

CREATE OR REPLACE MODEL `ensa-437407.ensa.xsell_rf_classifier_model`
OPTIONS(
  model_type='boosted_tree_classifier',
  num_parallel_tree=50,
  max_iterations=1,
  learn_rate=1.0,
  input_label_cols=['event_type']
) AS
SELECT
  event_type,
  customer_id,
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  coverage_count,
  total_premium
FROM `ensa-437407.ensa.ensa_xsell_events_training_data`
WHERE data_split = 'TRAIN'
and event_type is not null;

SELECT *
FROM ML.EVALUATE(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_model`,
  (
    SELECT
      event_type,
      customer_id,
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM `ensa-437407.ensa.ensa_xsell_events_training_data_stratified`
    WHERE data_split = 'TEST'
    and event_type is not null
  )
);
create or replace table `ensa-437407.ensa.xsell_rf_model_predictions` as
SELECT *
FROM ML.PREDICT(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_model`,
  (
    SELECT
      customer_id,
      event_type,
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
);


-- Combining_datasets
--Previous trained not usable
SELECT
  Customer_ID,
  predicted_label AS upsell_prediction,
  (SELECT prob FROM UNNEST(predicted_label_probs) WHERE label = predicted_label) AS upsell_confidence
FROM ML.PREDICT(MODEL `ensa-437407.ensa.upsell_model_rf`,
  (SELECT
    Customer_ID,
    frequency,
    monetary,
    recency,
    claim_activity
   FROM `ensa-437407.ensa.customer_features_test`));

-- Combining Datasets

-- Deduplicate `customer_features_test`
CREATE OR REPLACE TABLE `ensa-437407.ensa.combined_training_data` AS
WITH deduplicated_cf AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY recency DESC) AS row_num
    FROM `ensa.customer_features`
  )
  WHERE row_num = 1
),

-- Deduplicate `ensa_xsell_events_training_data`
deduplicated_xs AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY policy_count DESC) AS row_num
    FROM `ensa.ensa_xsell_events`
  )
  WHERE row_num = 1
)

-- Join the deduplicated datasets
SELECT
  cf.Customer_ID,
  cf.frequency,
  cf.monetary,
  cf.recency,
  cf.claim_activity,
  cf.upsell_opp,
  xs.event_type,
  xs.policy_type,
  xs.previous_policy_type,
  xs.previous_month_premium,
  xs.previous_month_coverage_cnt,
  xs.previous_month_policy_cnt,
  xs.policy_count,
  xs.coverage_count,
  xs.total_premium
FROM deduplicated_cf AS cf
LEFT JOIN deduplicated_xs AS xs
ON cf.Customer_ID = xs.Customer_ID;

-- Stratified Training Splits on Combined Data

CREATE OR REPLACE TABLE `ensa.ensa_xsell_events_training_data_stratified` AS
SELECT *,
  CASE
    WHEN RAND() < 0.8 THEN 'TRAIN'
    ELSE 'TEST'
  END AS data_split
FROM (
  SELECT *, RAND() AS random_value
  FROM `ensa-437407.ensa.ensa_xsell_events`
 -- WHERE event_type IS NOT NULL
)
QUALIFY NTILE(2) OVER (PARTITION BY event_type ORDER BY random_value) = 1;

SELECT event_type, COUNT(*) AS count
FROM `ensa-437407.ensa.combined_training_data_stratified`
WHERE data_split = 'TEST'
GROUP BY event_type;

SELECT event_type, COUNT(*) AS count
FROM `ensa-437407.ensa.combined_training_data_stratified`
WHERE data_split = 'TRAIN'
GROUP BY event_type;

-- Do nothing model
CREATE OR REPLACE MODEL `ensa-437407.ensa.xsell_rf_classifier_with_donothing`
OPTIONS(
  model_type='boosted_tree_classifier',
  num_parallel_tree=50,
  max_iterations=1,
  learn_rate=1.0,
  input_label_cols=['event_type']
) AS
SELECT
  CASE
    WHEN event_type IS NULL THEN 'Do Nothing'
    ELSE event_type
  END AS event_type,  -- Map NULL to "Do Nothing"
  customer_id,
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  coverage_count,
  total_premium
FROM `ensa-437407.ensa.ensa_xsell_events_training_data_stratified_do_null`
WHERE data_split = 'TRAIN';

-- Evaluate do nothing model
select *
from ml.evaluate(
  model `ensa-437407.ensa.xsell_rf_classifier_with_donothing`,
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
      total_premium
    from `ensa.ensa_xsell_events_training_data_stratified_do_null`
    where data_split = 'TEST'
  )
);
-- rf model predictions on do nothing
create or replace table ensa.xsell_rf_predictions_w_donothing as
select
  customer_id,
  predicted_event_type AS action,  -- Predicted action: "Do Nothing", "Upsell", or "Cross-sell"
  (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Do Nothing') AS do_nothing_prob,
  (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Upsell') AS upsell_prob,
  (SELECT prob FROM UNNEST(predicted_event_type_probs) WHERE label = 'Cross-sell') AS cross_sell_prob
FROM ML.PREDICT(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_with_donothing`,
  (
    SELECT
      customer_id,
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
);


SELECT *
FROM ML.PREDICT(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`,
  (
    SELECT
      customer_id,
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
LIMIT 10;


CREATE OR REPLACE MODEL `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`
OPTIONS(
  model_type='boosted_tree_classifier',
  num_parallel_tree=50,
  max_iterations=5,
  learn_rate=1.0,
  input_label_cols=['event_type']
) AS
SELECT
  CASE
    WHEN event_type IS NULL THEN 'Do Nothing'  -- Ensure NULL values are mapped to "Do Nothing"
    WHEN DATE_DIFF(`date`, CURRENT_DATE(), DAY) < 0 THEN
      CASE
        WHEN event_type = 'Do Nothing' THEN 'Cross-sell'  -- Override to "Cross-sell" if negative
        ELSE event_type
      END
    ELSE event_type  -- Keep the original event_type if Days_to_Renewal is non-negative
  END AS event_type,
  DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,  -- Include the feature
  policy_type,
  previous_policy_type,
  previous_month_premium,
  previous_month_coverage_cnt,
  previous_month_policy_cnt,
  policy_count,
  coverage_count,
  total_premium
FROM `ensa.combined_training_data_stratified`
WHERE data_split = 'TRAIN';

SELECT *
FROM ML.EVALUATE(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`,
  (
    SELECT
      CASE
        WHEN event_type IS NULL THEN 'Do Nothing'  -- Ensure nulls are handled for evaluation
        ELSE event_type
      END AS event_type,
      DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,  -- Include Renewal Days feature
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium
    FROM `ensa-437407.ensa.ensa_xsell_events_training_data_stratified`
    WHERE data_split = 'TEST'  -- Use the test split for evaluation
  )
);

create or replace table ensa.rf_predictions_with_renewal_days as
SELECT
  *,
  CASE
    WHEN DATE_DIFF(`date`, CURRENT_DATE(), DAY) < 0 THEN
      CASE
        WHEN predicted_event_type = 'Do Nothing' THEN 'Cross-sell'  -- Override prediction
        ELSE predicted_event_type
      END
    ELSE predicted_event_type  -- Keep the original prediction
  END AS final_prediction  -- Adjust predictions based on business rules
FROM ML.PREDICT(
  MODEL `ensa-437407.ensa.xsell_rf_classifier_with_days_to_renewal`,
  (
    SELECT
      DATE_DIFF(`date`, CURRENT_DATE(), DAY) AS Days_to_Renewal,  -- Derive Renewal Days
      policy_type,
      previous_policy_type,
      previous_month_premium,
      previous_month_coverage_cnt,
      previous_month_policy_cnt,
      policy_count,
      coverage_count,
      total_premium,
      date
    FROM `ensa-437407.ensa.ensa_xsell_events`
  )
);

