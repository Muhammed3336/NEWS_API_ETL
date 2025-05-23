CREATE OR REPLACE DATABASE my_database;
USE DATABASE my_database;
CREATE OR REPLACE SCHEMA TableSchema;
USE SCHEMA TableSchema;
CREATE OR REPLACE TABLE NewsArticle (
    source_name VARCHAR,
    author VARCHAR,
    title VARCHAR,
    description VARCHAR,
    url VARCHAR,
    urlToImage VARCHAR,
    publishedAt TIMESTAMP_NTZ,
    content VARCHAR,
    sentiment FLOAT
);


  CREATE OR REPLACE STORAGE INTEGRATION Awss3_Integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::203520860673:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://newsapikafka/output/');

  
  CREATE OR REPLACE STAGE My_s3_Stagess
  URL = 's3://newsapikafka/output/'
  STORAGE_INTEGRATION = Awss3_Integration;

 CREATE OR REPLACE FILE FORMAT My_Csv_Formatsss
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', '')
    TRIM_SPACE = TRUE;

-- COPY INTO news_table
-- FROM @my_s3_stage/kafka_news_data/part-00000-ea8a403d-173b-4a90-a966-cab962e3eab3-c000.csv
-- FILE_FORMAT = my_csv_format
-- ON_ERROR = 'CONTINUE';

COPY INTO NewsArticle
FROM @My_s3_Stagess
FILE_FORMAT = My_Csv_Formatsss
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';




DESC INTEGRATION awss3_integration;

SELECT * FROM NewsArticle;

LIST @My_s3_Stagess;