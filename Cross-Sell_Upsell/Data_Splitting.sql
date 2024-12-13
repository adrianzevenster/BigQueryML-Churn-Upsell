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