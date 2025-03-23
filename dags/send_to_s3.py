from airflow import DAG
from datetime import timedelta, datetime
from io import StringIO
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import pandas as pd
from requests.exceptions import HTTPError, Timeout, RequestException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # pip install apache-airflow-providers-amazon
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.email import EmailOperator


# the url
url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"


# Airflow S3 connection ID (set in Airflow Admin)
S3_CONN_ID = "s3_conn"
S3_BUCKET_NAME = "thisismydemos3bucketnew"


# file paths for csv files that will be created 
S3_FILE_KEY_dim_period = 'load_to_snowflake_from_s3_bucket/dim_period_folder/dim_period.csv'
S3_FILE_KEY_dim_location = "load_to_snowflake_from_s3_bucket/dim_location_folder/dim_location.csv"
S3_FILE_KEY_dim_vehicle = "load_to_snowflake_from_s3_bucket/dim_vehicle_folder/dim_vehicle.csv"
S3_FILE_KEY_dim_contributing_factor = "load_to_snowflake_from_s3_bucket/dim_contributing_factor_folder/dim_contributing_factor.csv"
S3_FILE_KEY_fact_crashes = "load_to_snowflake_from_s3_bucket/fact_crashes_folder/fact_crashes.csv"


# Define column lists   (we are going to merge them with the fact table on the 'collision_id'
dim_period_column_list = ['collision_id', 'crash_date', 'crash_time']
dim_location_column_list = ['collision_id', 'zip_code', 'latitude', 'longitude', 'borough', 'on_street_name',
                            'off_street_name', 'cross_street_name']
dim_vehicle_column_list = ['collision_id', 'vehicle_type_code1', 'vehicle_type_code2', 'vehicle_type_code_3',
                           'vehicle_type_code_4', 'vehicle_type_code_5']
dim_contributing_factor_column_list = ['collision_id', 'contributing_factor_vehicle_1', 'contributing_factor_vehicle_2',
                                       'contributing_factor_vehicle_3', 'contributing_factor_vehicle_4',
                                       'contributing_factor_vehicle_5']
fact_crashes_column_list = ['collision_id', 'number_of_persons_injured', 'number_of_persons_killed',
                            'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
                            'number_of_cyclist_injured', 'number_of_cyclist_killed', 'number_of_motorist_injured',
                            'number_of_motorist_killed', 'crash_period_id', 'location_id', 'vehicle_id', 'factor_id']


# function to extract the data 
def extract_data_from_api():
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        print("Data successfully extracted!")
        df = pd.DataFrame(data).where(pd.notna(pd.DataFrame(data)), None)
        # Drop the location column if it exists (this column is excluded)
        if 'location' in df.columns:
            df.drop(columns=['location'], inplace=True)  

        df = df.drop_duplicates()  # removing duplicates
        return df
    except HTTPError as e:
        print(f"HTTP error occurred: {e.response.status_code} - {e.response.reason}")
    except Timeout:
        print("Request timed out. Try again later.")
    except RequestException as e:
        print(f"API request failed: {e}")


# Transform Functions
def transform_dim_period():
    df = extract_data_from_api()
    df = df[dim_period_column_list]
    df.insert(0, 'crash_period_id', range(100, len(df) + 100))
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')

    # adding Year, weekday and Month columns
    df['crash_year'] = df['crash_date'].dt.year
    df['crash_month'] = df['crash_date'].dt.month
    df['crash_weekday'] = df['crash_date'].dt.day_name()  # Get weekday name

    # Convert 'crash_date' to iso format before 
    df['crash_date'] = df['crash_date'].dt.strftime('%Y-%m-%d')

    # Drop the 'location' column if it exists
    if 'location' in df.columns:
        df = df.drop(columns=['location'])
        print("Dim period data is ready for further processing.")
    return df  


def transform_dim_location():
    df = extract_data_from_api()
    df = df[dim_location_column_list]
    df.insert(0, 'location_id', range(100, len(df) + 100))

    # Drop the 'location' column if it exists
    if 'location' in df.columns:
        df = df.drop(columns=['location'])
        print("Dim location data is ready for further processing.")
    return df   


def transform_dim_vehicle():
    df = extract_data_from_api()
    df = df[dim_vehicle_column_list]
    df.insert(0, 'vehicle_id', range(100, len(df) + 100))

    # Drop the 'location' column if it exists
    if 'location' in df.columns:
        df = df.drop(columns=['location'])
        print("Dim vehicle data is ready for further processing.")
    return df  


def transform_dim_contributing_factor():
    df = extract_data_from_api()
    df = df[dim_contributing_factor_column_list]
    df.insert(0, 'factor_id', range(100, len(df) + 100))

    # Drop the 'location' column if it exists
    if 'location' in df.columns:
        df = df.drop(columns=['location'])       
        print("Dim Location data is ready for further processing.")
    return df  


# Fact Table Transformation
def transform_fact_crashes():
    fact_crashes = extract_data_from_api()

    # calling all the functions to get the returned data frames to merge with the fact df for foreign keys
    dim_period = transform_dim_period()
    dim_location = transform_dim_location()
    dim_vehicle = transform_dim_vehicle()
    dim_contributing_factor = transform_dim_contributing_factor()  

    # Merging Fact Table with Dimensions to get foreign keys
    fact_crashes = fact_crashes.merge(dim_period, on='collision_id', how='inner')
    fact_crashes = fact_crashes.merge(dim_location, on='collision_id', how='inner')
    fact_crashes = fact_crashes.merge(dim_vehicle, on='collision_id', how='inner')
    fact_crashes = fact_crashes.merge(dim_contributing_factor, on='collision_id', how='inner')

    # Selecting required columns for fact table
    fact_crashes = fact_crashes[fact_crashes_column_list]

    # Renaming primary key column
    fact_crashes.rename(columns={'collision_id': 'crash_id'}, inplace=True)
    
    # drop the rows with no values (which are going to be used as primary and foreign keys
    print("Fact crash data is ready for further processing.")
    return fact_crashes

# load functions

def load_dim_period():
    # To maintain data integrity, (to avoid loading data that already exists in the S3 file).
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # Airflow's S3 connection
    # Checking if file exists in S3
    if s3_hook.check_for_key(S3_FILE_KEY_dim_period, bucket_name=S3_BUCKET_NAME):
        # Read existing data from S3
        existing_file = s3_hook.read_key(S3_FILE_KEY_dim_period, bucket_name=S3_BUCKET_NAME)  
        existing_data = pd.read_csv(StringIO(existing_file))  # Convert the CSV string into a Pandas DataFrame

    else:
        existing_data = pd.DataFrame()  # If file doesn't exist, then create an empty DataFrame

    # Generate new data (call the function and store the returned data into new_data variable)
    new_data = transform_dim_period()    # removing the collision column

    # Drop the 'location' column if it exists
    if 'collision_id' in new_data.columns:
        new_data = new_data.drop(columns=['collision_id']) 

    # Combine and remove duplicates
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()  # creating a buffer to store the csv 
    combined_data.to_csv(csv_buffer, index=False)  
    
    # Upload back to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),  # extracts the CSV content from the "StringIO" buffer

        key=S3_FILE_KEY_dim_period,
        bucket_name=S3_BUCKET_NAME,
        replace=True  # Overwrite existing file
    )

    print("dim_period data successfully updated in S3!")


def load_dim_location():
    # To maintain data integrity, (to avoid loading data that already exists in the S3 file).
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # Airflow's S3 connection
    # Checking if file exists in S3
    if s3_hook.check_for_key(S3_FILE_KEY_dim_location, bucket_name=S3_BUCKET_NAME):
        # Read existing data from S3
        existing_file = s3_hook.read_key(S3_FILE_KEY_dim_location, bucket_name=S3_BUCKET_NAME)  
        existing_data = pd.read_csv(StringIO(existing_file))  # Convert the CSV string into a Pandas DataFrame

    else:
        existing_data = pd.DataFrame()  # If file doesn't exist, then create an empty DataFrame

    # Generate new data 
    new_data = transform_dim_location()
    
    if 'collision_id' in new_data.columns:
        new_data = new_data.drop(columns=['collision_id']) 

    # Combine and remove duplicates
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()  # create a buffer to store the csv 
    combined_data.to_csv(csv_buffer, index=False)  
    
    # Upload back to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),  # extracts the CSV content from the "StringIO" buffer 

        key=S3_FILE_KEY_dim_location,
        bucket_name=S3_BUCKET_NAME,
        replace=True  # Overwrite existing file
    )

    print("dim_location data successfully updated in S3!")


def load_dim_vehicle():
    # To maintain data integrity, (to avoid loading data that already exists in the S3 file).
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # Use Airflow's S3 connection
    # Checking if file exists in S3
    if s3_hook.check_for_key(S3_FILE_KEY_dim_vehicle, bucket_name=S3_BUCKET_NAME):
        # Read existing data from S3
        existing_file = s3_hook.read_key(S3_FILE_KEY_dim_vehicle, bucket_name=S3_BUCKET_NAME) 

        existing_data = pd.read_csv(StringIO(existing_file))  # Convert the CSV string into a Pandas DataFrame

    else:
        existing_data = pd.DataFrame()  # If file doesn't exist, then create an empty DataFrame

    # Generate new data 
    new_data = transform_dim_vehicle()

    if 'collision_id' in new_data.columns:
        new_data = new_data.drop(columns=['collision_id']) 

    # Combine and remove duplicates
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()  
    combined_data.to_csv(csv_buffer, index=False)  

    # Upload back to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),  # extracts the CSV content from the "StringIO" buffer 

        key=S3_FILE_KEY_dim_vehicle,
        bucket_name=S3_BUCKET_NAME,
        replace=True  # Overwrite existing file
    )

    print("dim_vehicle data successfully updated in S3!")


def load_dim_contributing_factor():
    # To maintain data integrity, (to avoid loading data that already exists in the S3 file).
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # Airflow's S3 connection
    # Checking if file exists in S3
    if s3_hook.check_for_key(S3_FILE_KEY_dim_contributing_factor, bucket_name=S3_BUCKET_NAME):
        # Read existing data from S3
        existing_file = s3_hook.read_key(S3_FILE_KEY_dim_contributing_factor, bucket_name=S3_BUCKET_NAME)  

        existing_data = pd.read_csv(StringIO(existing_file))  # Convert the CSV string into a Pandas DataFrame

    else:
        existing_data = pd.DataFrame()  # If file doesn't exist, then create an empty DataFrame

    # Generate new data 
    new_data = transform_dim_contributing_factor()

    if 'collision_id' in new_data.columns:
        new_data = new_data.drop(columns=['collision_id']) 

    # Combine and remove duplicates
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

    # Convert DataFrame to CSV
    csv_buffer = StringIO() 
    combined_data.to_csv(csv_buffer, index=False) 

    # Upload back to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),  # extracts the CSV content from the "StringIO" buffer 

        key=S3_FILE_KEY_dim_contributing_factor,
        bucket_name=S3_BUCKET_NAME,
        replace=True  # Overwrite existing file
    )

    print("dim_contributing_factor data successfully updated in S3!")


def load_fact_crashes():
    # To maintain data integrity, (to avoid loading data that already exists in the S3 file).
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)  # Use Airflow's S3 connection
    # Checking if file exists in S3
    if s3_hook.check_for_key(S3_FILE_KEY_fact_crashes, bucket_name=S3_BUCKET_NAME):
        # Read existing data from S3
        existing_file = s3_hook.read_key(S3_FILE_KEY_fact_crashes, bucket_name=S3_BUCKET_NAME)  
        existing_data = pd.read_csv(StringIO(existing_file))  # Convert the CSV string into a Pandas DataFrame

    else:
        existing_data = pd.DataFrame()  # If file doesn't exist, then create an empty DataFrame

    # Generate new data 
    new_data = transform_fact_crashes()
    if 'collision_id' in new_data.columns:
        new_data = new_data.drop(columns=['collision_id']) 

    # Combine and remove duplicates
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

    # Convert DataFrame to CSV
    csv_buffer = StringIO()  
    combined_data.to_csv(csv_buffer, index=False)  

    # Upload back to S3
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),  # extracts the CSV content from the "StringIO" buffer 

        key=S3_FILE_KEY_fact_crashes,
        bucket_name=S3_BUCKET_NAME,
        replace=True  # Overwrite existing file
    )
    print("fact_crashes data successfully updated in S3!")

# default argument
default_args = {
    'owner': 'fabrice selemani',
    'start_date': datetime(2025, 3, 19),
    'retry': 1,  
    'retry_delay': timedelta(minutes=1) 
}
with DAG(
        dag_id='load_data_to_s3_bucket',
        default_args=default_args,
        schedule="@daily",
        catchup=False
) as dag:
    

    start = EmptyOperator(task_id='start', dag=dag)
    
    # extracting data task
    extract_data_task = PythonOperator(task_id='extract_data_from_api',
                                    python_callable=extract_data_from_api,
                                    dag=dag)
    
    # transform dataframe tasks
    dim_period_task = PythonOperator(task_id='transform_dim_period',
                                    python_callable=transform_dim_period,
                                    dag=dag)
    
    dim_location_task = PythonOperator(task_id='transform_dim_location',
                                    python_callable=transform_dim_location,
                                    dag=dag)
    
    dim_vehicle_task = PythonOperator(task_id='transform_dim_vehicle',
                                    python_callable=transform_dim_vehicle,
                                    dag=dag)
    
    dim_contr_fac_task = PythonOperator(task_id='transform_dim_contributing_factor',
                                    python_callable=transform_dim_contributing_factor,
                                    dag=dag)
    
    fact_crashes_task = PythonOperator(task_id='transform_fact_crashes',
                                    python_callable=transform_fact_crashes,
                                    trigger_rule="all_success",
                                    dag=dag)
    
    # load data to s3 bucket tasks
    load_dim_period = PythonOperator(task_id='load_dim_period',
                                    python_callable=load_dim_period,
                                    dag=dag)
    
    load_dim_location = PythonOperator(task_id='load_dim_location',
                                    python_callable=load_dim_location,
                                    dag=dag)
    
    load_dim_vehicle = PythonOperator(task_id='load_dim_vehicle',
                                    python_callable=load_dim_vehicle,
                                    dag=dag)
    
    load_dim_contr_fac = PythonOperator(task_id='load_dim_contributing_factor',
                                    python_callable=load_dim_contributing_factor,
                                    dag=dag)
    
    load_fact_crashes = PythonOperator(task_id='load_fact_crashes',
                                    python_callable=load_fact_crashes,
                                    dag=dag)
    
    # sensor tasks 
    wait_dim_per_file = S3KeySensor( task_id='wait_for_the_dim_period_file',
                                       bucket_name=S3_BUCKET_NAME,
                                       bucket_key=S3_FILE_KEY_dim_period,
                                       aws_conn_id=S3_CONN_ID,
                                       poke_interval=60,
                                       timeout=360, 
                                       dag=dag)
    
    wait_dim_loc_file = S3KeySensor( task_id='wait_for_the_dim_location_file',
                                       bucket_name=S3_BUCKET_NAME,
                                       bucket_key=S3_FILE_KEY_dim_location,
                                       aws_conn_id=S3_CONN_ID,
                                       poke_interval=60,
                                       timeout=360, 
                                       dag=dag)
    
    wait_dim_veh_file = S3KeySensor( task_id='wait_for_the_dim_vehicle_file',
                                       bucket_name=S3_BUCKET_NAME,
                                       bucket_key=S3_FILE_KEY_dim_vehicle,
                                       aws_conn_id=S3_CONN_ID,
                                       poke_interval=60,
                                       timeout=360, 
                                       dag=dag)
    
    wait_dim_contr_fac_file = S3KeySensor( task_id='wait_for_the_dim_contributing_factor_file',
                                       bucket_name=S3_BUCKET_NAME,
                                       bucket_key=S3_FILE_KEY_dim_contributing_factor,
                                       aws_conn_id=S3_CONN_ID,
                                       poke_interval=60,
                                       timeout=360, 
                                       dag=dag)
    
    wait_fact_cr_file = S3KeySensor( task_id='wait_for_the_fact_crashes_file',
                                       bucket_name=S3_BUCKET_NAME,
                                       bucket_key=S3_FILE_KEY_fact_crashes,
                                       aws_conn_id=S3_CONN_ID,
                                       poke_interval=60,
                                       timeout=360,
                                       dag=dag)
    
    # email notification tasks
    send_success_email = EmailOperator(task_id='send_email_success', 
                                       to= 'fabriceselemani96@gmail.com',
                                       subject='Etl process success notication',
                                       html_content='<h3>hey, The load data to s3 bucket Etl process has succeed.<h3>',
                                       trigger_rule='all_success',
                                       dag=dag)
    
    send_failure_email = EmailOperator(task_id='send_email_failure', 
                                       to= 'fabriceselemani96@gmail.com',
                                       subject='Etl process failure notication',
                                       html_content='<h3>hey, The load data to s3 bucket Etl process has failed. check the logs for more info.<h3>',
                                       trigger_rule='one_failed',
                                       dag=dag,)
    
    # confirmation task
    confirm_data_load = EmptyOperator(task_id='confirm_data_load',
                                      dag=dag)

    

# task dependencies 
start >> extract_data_task >> [dim_period_task, dim_location_task, dim_vehicle_task, dim_contr_fac_task] >> fact_crashes_task  

fact_crashes_task >> load_dim_period >> wait_dim_per_file  
fact_crashes_task >> load_dim_location >> wait_dim_loc_file  
fact_crashes_task >> load_dim_vehicle >> wait_dim_veh_file  
fact_crashes_task >> load_dim_contr_fac >> wait_dim_contr_fac_file  
fact_crashes_task >> load_fact_crashes >> wait_fact_cr_file  

[wait_dim_per_file, wait_dim_loc_file, wait_dim_veh_file, wait_dim_contr_fac_file, wait_fact_cr_file] >> confirm_data_load  

confirm_data_load >> send_success_email  
confirm_data_load >> send_failure_email

