CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
  customerName              STRING,
  email                     STRING,
  phone                     STRING,
  birthDay                  STRING,
  serialNumber              STRING,
  registrationDate          BIGINT,
  lastUpdateDate            BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate   BIGINT,
  shareWithFriendsAsOfDate  BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-365869683794/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
