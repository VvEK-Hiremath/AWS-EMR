-- Summary: This sample shows you how to analyze CloudFront logs stored in S3 using Hive
SET mapred.reduce.tasks = 1;
-- Create table using sample data in S3.  Note: you can replace this S3 path with your own.
CREATE EXTERNAL TABLE IF NOT EXISTS `cloudfront_logs` (
  `date` Date,
  `time` STRING,
  `location` STRING,
  `bytes` INT,
  `requestip` STRING,
  `method` STRING,
  `host` STRING,
  `uri` STRING,
  `status` INT,
  `referrer` STRING,
  `os` String,
  `browser` String,
  `browserversion` String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "^(?!#)([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+[^\(]+[\(]([^\;]+).*\%20([^\/]+)[\/](.*)$"
) LOCATION 's3a://${BUCKET}/data';
-- Total requests per operating system for a given time frame
INSERT OVERWRITE DIRECTORY 's3a://${BUCKET}/output/os_requests'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT `os`, COUNT(*) count FROM `cloudfront_logs` GROUP BY `os`;
