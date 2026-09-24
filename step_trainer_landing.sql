CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorReadingTime  BIGINT,
  serialNumber       STRING,
  distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-365869683794/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
