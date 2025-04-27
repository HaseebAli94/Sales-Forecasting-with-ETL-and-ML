from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
import pyodbc

CSV_PATH = 'perfume_sales_data.csv'
SERVER = 'DESKTOP-H7OKNFO'      
DATABASE = 'PerfumeSales'  
USERNAME = 'sa'
PASSWORD = '*********'
TABLE_NAME = 'perfume_sales'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def generate_fake_data(): # here using the faker library to create the fake data for this pipeline but for actual application here can linked the Shopify or any other ecommerce platform 
    fake = Faker()
    data = []

    for _ in range(100):  
        perfume_name = fake.word().capitalize() + " Perfume"
        sale_date = fake.date_this_year()
        units_sold = fake.random_int(min=1, max=100)
        revenue = round(units_sold * fake.random_number(digits=2), 2)
        data.append((perfume_name, sale_date, units_sold, revenue))

    df = pd.DataFrame(data, columns=["PerfumeName", "SaleDate", "UnitsSold", "Revenue"])
    df.to_csv(CSV_PATH, index=False)
    print(f"Generated fake data and saved to {CSV_PATH}")

def create_connection(): # this function is to create the connection with the sql server using pyodbc 
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={USERNAME};'
        f'PWD={PASSWORD}'
    )
    return pyodbc.connect(conn_str)

def load_data_into_sql(): # this function loads the CSV data into sql server
    df = pd.read_csv(CSV_PATH)
    conn = create_connection()
    cursor = conn.cursor()

    try:
        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {TABLE_NAME} (PerfumeName, SaleDate, UnitsSold, Revenue)
                VALUES (?, ?, ?, ?)
            """
            cursor.execute(insert_query, row['PerfumeName'], row['SaleDate'], row['UnitsSold'], row['Revenue'])
        
        conn.commit()
        print(f" Inserted {len(df)} records into {TABLE_NAME}")
    
    except Exception as e:
        print(f" Error: {e}")
    
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id='perfume_sales_pipeline',
    default_args=default_args,
    description='Generate fake perfume sales data and load into SQL Server',
    schedule_interval=timedelta(minutes=1440),  
    start_date=datetime(2025, 3, 16),
    catchup=False,
    tags=['perfume', 'sales', 'sqlserver'],
) as dag:

    generate_data = PythonOperator(
        task_id='generate_fake_perfume_data',
        python_callable=generate_fake_data
    )

    load_data = PythonOperator(
        task_id='load_perfume_data_to_sqlserver',
        python_callable=load_data_into_sql
    )
    generate_data >> load_data
