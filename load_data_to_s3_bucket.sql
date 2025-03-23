-- creating the database
CREATE DATABASE mydb;

-- selecting the database 
USE mydb;


-- Dimension Table: dim_period
CREATE OR REPLACE TABLE dim_period (
    crash_period_id INT ,
    crash_date DATE,
    crash_time TIME,
    crash_year INT,
    crash_month INT,
    crash_weekday VARCHAR(50),
    PRIMARY KEY (crash_period_id)  -- Note: Snowflake does not enforce primary keys
);

-- Dimension Table: dim_location
CREATE OR REPLACE TABLE dim_location (
    location_id INT,
    zip_code VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT,
    borough VARCHAR(255),
    on_street_name VARCHAR(255),
    off_street_name VARCHAR(255),
    cross_street_name VARCHAR(255),
    PRIMARY KEY (location_id)
);

-- Dimension Table: dim_vehicle
CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_id INT,
    vehicle_type_code1 VARCHAR(100),
    vehicle_type_code2 VARCHAR(100),
    vehicle_type_code_3 VARCHAR(100),
    vehicle_type_code_4 VARCHAR(100),
    vehicle_type_code_5 VARCHAR(100),
    PRIMARY KEY (vehicle_id)
);

-- Dimension Table: dim_contributing_factor
CREATE OR REPLACE TABLE dim_contributing_factor (
    factor_id INT,
    contributing_factor_vehicle_1 VARCHAR(255),
    contributing_factor_vehicle_2 VARCHAR(255),
    contributing_factor_vehicle_3 VARCHAR(255),
    contributing_factor_vehicle_4 VARCHAR(255),
    contributing_factor_vehicle_5 VARCHAR(255),
    PRIMARY KEY (factor_id)
);

-- Fact Table: fact_crashes
CREATE OR REPLACE TABLE fact_crashes (
    period INT,
    number_of_persons_injured INT,
    number_of_persons_killed INT,
    number_of_pedestrians_injured INT,
    number_of_pedestrians_killed INT,
    number_of_cyclist_injured INT,
    number_of_cyclist_killed INT,
    number_of_motorist_injured INT,
    number_of_motorist_killed INT,
    crash_period_id INT,
    location_id INT,
    vehicle_id INT,
    factor_id INT,
    PRIMARY KEY (collision_id),
    FOREIGN KEY (crash_period_id) REFERENCES dim_period(crash_period_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id),
    FOREIGN KEY (factor_id) REFERENCES dim_contributing_factor(factor_id)
);



show variables;

-- creating the file format 
CREATE OR REPLACE FILE FORMAT my_csv_format_for_s3_proj
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1                                                 
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE
TRIM_SPACE = TRUE;


-- creating an integretion 
CREATE OR REPLACE STORAGE INTEGRATION my_integration_for_s3
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN =$STORAGE_AWS_ROLE_ARN   
STORAGE_ALLOWED_LOCATIONS = ('s3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/')
COMMENT = 'this an integration for s3 for load_to_snowflake_from_s3_bucket';

-- -- to view the description of the integration 
-- DESC INTEGRATION my_integration_for_s3;




-- creating the stage for the dim period table 
CREATE OR REPLACE STAGE dim_period_stage
URL = 's3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/dim_period_folder/'
STORAGE_INTEGRATION = my_integration_for_s3
FILE_FORMAT = my_csv_format_for_s3_proj;

-- creating the stage for the dim location table
CREATE OR REPLACE STAGE dim_location_stage
URL = 's3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/dim_location_folder/'
STORAGE_INTEGRATION = my_integration_for_s3
FILE_FORMAT = my_csv_format_for_s3_proj;

-- creating the stage for the dim vehicle table 
CREATE OR REPLACE STAGE dim_vehicle_stage
URL = 's3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/dim_vehicle_folder/'
STORAGE_INTEGRATION = my_integration_for_s3
FILE_FORMAT = my_csv_format_for_s3_proj;

-- creating the stage for the dim contributing_factor table 
CREATE OR REPLACE STAGE dim_contributing_factor_stage
URL = 's3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/dim_contributing_factor_folder/'
STORAGE_INTEGRATION = my_integration_for_s3
FILE_FORMAT = my_csv_format_for_s3_proj;

-- creating the stage for the fact crashes table 
CREATE OR REPLACE STAGE fact_crashes_stage
URL = 's3://thisismydemos3bucketnew/load_to_snowflake_from_s3_bucket/fact_crashes_folder/'
STORAGE_INTEGRATION = my_integration_for_s3
FILE_FORMAT = my_csv_format_for_s3_proj;



-- create the pipe for the dim period table
CREATE OR REPLACE PIPE dim_period_pipe
AUTO_INGEST = TRUE
AS
COPY INTO dim_period
FROM @dim_period_stage
FILE_FORMAT = my_csv_format_for_s3_proj;

-- create the pipe for the dim location table
CREATE OR REPLACE PIPE dim_location_pipe
AUTO_INGEST = TRUE
AS
COPY INTO dim_location
FROM @dim_location_stage
FILE_FORMAT = my_csv_format_for_s3_proj;

-- create the pipe for the dim vehicle table
CREATE OR REPLACE PIPE dim_vehicle_pipe
AUTO_INGEST = TRUE
AS
COPY INTO dim_vehicle
FROM @dim_vehicle_stage
FILE_FORMAT = my_csv_format_for_s3_proj;


-- create the pipe for the dim contributing factor table
CREATE OR REPLACE PIPE dim_contributing_factor_pipe
AUTO_INGEST = TRUE
AS
COPY INTO dim_contributing_factor
FROM @dim_contributing_factor_stage
FILE_FORMAT = my_csv_format_for_s3_proj;


-- create the pipe for the fact crashes table
CREATE OR REPLACE PIPE fact_crashes_stage_pipe
AUTO_INGEST = TRUE
AS
COPY INTO fact_crashes
FROM @fact_crashes_stage
FILE_FORMAT = my_csv_format_for_s3_proj;

ALTER TABLE fact_crashes CLUSTER BY (period_id, location_id);

show pipes;




-- verify if data was loaded   
SELECT * FROM fact_crashes;
SELECT * FROM dim_contributing_factor;
SELECT * FROM dim_vehicle;
SELECT * FROM dim_location;
SELECT * FROM dim_period;
