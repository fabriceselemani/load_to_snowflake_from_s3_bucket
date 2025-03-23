# load_to_snowflake_from_s3_bucket

In this project, we extract data from the New York City API, process it, and load it into an S3 bucket. Then, we use the Snowflake COPY command to load the data into Snowflake tables.

We have also included the SQL scripts.

Here is the link to the data model (Entity Relationship Diagram) a simple star schema: [https://lucid.app/lucidchart/85eab406-cd0d-4a45-b128-f6c49b3ffdce/edit?viewport_loc=-39245%2C-4198%2C88434%2C46363%2C0_0&invitationId=inv_075f0135-37e1-4296-8440-6fa6d1bf9fb1].

Tools Used: Python, Apache Airflow, Docker, AWS S3, Snowflake, and AWS EventBridge (to notify Snowflake when new data is uploaded or updated).
 
