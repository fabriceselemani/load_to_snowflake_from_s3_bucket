from airflow import DAG
from datetime import datetime
import pandas as pd
from random import *
import numpy as np
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable



postgres_id = "postgres_conn"  # Postgres connection

# Fetch database credentials from Airflow Variables
POSTGRES_HOST = Variable.get("POSTGRES_HOST")
POSTGRES_PORT = Variable.get("POSTGRES_PORT")
POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD")
POSTGRES_DB = Variable.get("POSTGRES_DB")



# Define the connection string using environment variables
postgres_conn_string = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def tech_job():
    # create dataframe object
    df = pd.DataFrame()   # Empty data frame
    size = 100

    df['age'] = np.random.randint(0, 100, size)
    df['year_of_experience'] = np.random.randint(0, 9, size)
    df['favorite_job'] = np.random.choice(['Data engineer', 'Data_analyst', 'ML engineer'], size)
    df['favorite_city'] = np.random.choice(['Kinshasa', 'Dallas', 'Fort worth'], size)

    return df


def nursing():
    # create dataframe object
    df = pd.DataFrame()    # Empty data frame
    size = 100

    df['age'] = np.random.randint(0, 100, size)
    df['year_of_experience'] = np.random.randint(0, 9, size)
    df['favorite_job'] = np.random.choice(['Nurse_practitioner', 'Critical_care_nurse', 'Home_health_nurse'], size)
    df['favorite_city'] = np.random.choice(['Kinshasa', 'Dallas', 'Fort worth'], size)

    return df


def business():
    # create dataframe object
    df = pd.DataFrame()      # Empty data frame
    size = 100

    df['age'] = np.random.randint(0, 100, size)
    df['year_of_experience'] = np.random.randint(0, 9, size)
    df['favorite_job'] = np.random.choice(['Human_resource', 'Business_analyst', 'Manager'], size)
    df['favorite_city'] = np.random.choice(['Kinshasa', 'Dallas', 'Fort worth'], size)

    return df


def transform_tech_job():
    table = tech_job()
    table.dropna(inplace=True)
    table.drop_duplicates(inplace=True)

    return table


def transform_nursing():
    table = nursing()
    table.dropna(inplace=True)
    table.drop_duplicates(inplace=True)

    return table


def transform_business():
    table = business()
    table.dropna(inplace=True)
    table.drop_duplicates(inplace=True)

    return table





def send_tech_job():
    table = transform_tech_job()
    engine = create_engine(postgres_conn_string)
    try:
        table.to_sql('tech_job_table', engine, if_exists='append', index=False, schema='public', method='multi')   # append : if data exists append
        print("Data successfully inserted into tech_job_table")
    except Exception as e:
        print(f"Error inserting into tech_job_table: {e}")

    


def send_nursing():
    table = transform_nursing()
    engine = create_engine(postgres_conn_string)
    try:
        table.to_sql('nursing_table', engine, if_exists='append', index=False, schema='public', method='multi')   # append : if data exists append
        print("Data successfully inserted into tech_job_table")
    except Exception as e:
        print(f"Error inserting into tech_job_table: {e}")


def send_business():
    table = transform_business()
    engine = create_engine(postgres_conn_string)
    try:
        table.to_sql('business_table', engine, if_exists='append', index=False, schema='public', method='multi')  # append : if data exists append
        print("Data successfully inserted into tech_job_table")

    except Exception as e:
        print(f"Error inserting into tech_job_table: {e}")




default_args = {
    'owner': 'fabrice selemani',
    'start_date': datetime(2025, 1, 27)
}

with DAG(
        dag_id='generate_transform_store_profession_data',
        default_args=default_args,
        schedule='@daily',
        catchup=False
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # create dataframe tasks
    create_df_tech = PythonOperator(task_id='create_tech_job_dataframe',
                                    python_callable=tech_job)
    
    create_df_nursing = PythonOperator(task_id='create_nursing_dataframe',
                                    python_callable=nursing)
    
    create_df_business = PythonOperator(task_id='create_business_dataframe',
                                    python_callable=business)
    


    # tranforming tasks
    tranform_df_tech = PythonOperator(task_id='tranforming_tech_job_dataframe',
                                    python_callable=transform_tech_job)
    
    tranform_df_nursing = PythonOperator(task_id='tranforming_nursing_dataframe',
                                    python_callable=transform_nursing)
    
    tranform_df_business = PythonOperator(task_id='tranforming_business_dataframe',
                                    python_callable=transform_business)
    

    # create tables 
    create_table_tech = PostgresOperator(task_id='create_tech_job_table',
                                         postgres_conn_id=postgres_id,
                                         sql=""" 
                                            CREATE TABLE IF NOT EXISTS tech_job_table (
                                                  id SERIAL PRIMARY KEY,
                                                  age INT,
                                                  year_of_experience INT,
                                                  favorite_job VARCHAR(250),
                                                  favorite_city VARCHAR(250)

                                         )"""

    )


    create_table_nursing = PostgresOperator(task_id='create_nursing_table',
                                         postgres_conn_id=postgres_id,
                                         sql=""" 
                                            CREATE TABLE IF NOT EXISTS nursing_table (
                                                  id SERIAL PRIMARY KEY,
                                                  age INT,
                                                  year_of_experience INT,
                                                  favorite_job VARCHAR(250),
                                                  favorite_city VARCHAR(250)

                                         )"""

    )



    create_table_business = PostgresOperator(task_id='create_business_table',
                                         postgres_conn_id=postgres_id,
                                         sql=""" 
                                            CREATE TABLE IF NOT EXISTS business_table (
                                                  id SERIAL PRIMARY KEY,
                                                  age INT,
                                                  year_of_experience INT,
                                                  favorite_job VARCHAR(250),
                                                  favorite_city VARCHAR(250)

                                         )"""

    )

    

    # send to our local computer tasks
    send_df_tech = PythonOperator(task_id='sending_tech_job_dataframe_local_pc',
                                    python_callable=send_tech_job)
    
    send_df_nursing = PythonOperator(task_id='sending_nursing_dataframe_local_pc',
                                    python_callable=send_nursing)
    
    send_df_business = PythonOperator(task_id='sending_business_dataframe_local_pc',
                                    python_callable=send_business)
    
                                      

    start >> create_df_tech >> tranform_df_tech >> create_table_tech >> send_df_tech >> end
    start >> create_df_nursing >> tranform_df_nursing >> create_table_nursing >> send_df_nursing >> end
    start >> create_df_business >> tranform_df_business >> create_table_business >> send_df_business >> end










    



                                                    


            










    