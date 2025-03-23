# load_to_snowflake_from_s3_bucket

In this project, we extract data from the New York City API, process it, and load it into an S3 bucket. Then, we use the Snowflake COPY command to load the data into Snowflake tables.

We have also included the SQL scripts.

Here is the link to the data model (Entity Relationship Diagram) a simple star schema: [https://lucid.app/lucidchart/67fef1f2-72e6-47e6-b365-4d10a3949b7c/edit?invitationId=inv_8ac4b38f-3013-4ccb-bf68-bd0187823ce5].

Tools Used: Python, Apache Airflow, Docker, AWS S3, Snowflake, and AWS EventBridge (to notify Snowflake when new data is uploaded or updated).
 
