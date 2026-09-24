CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  `user`      STRING,
  `timestamp` BIGINT,
  x           DOUBLE,
  y           DOUBLE,
  z           DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-365869683794/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
