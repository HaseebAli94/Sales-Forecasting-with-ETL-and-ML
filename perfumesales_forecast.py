# code for the airflow DAG for the ML based Forecasting
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from prophet import Prophet
import pandas as pd
from difflib import get_close_matches
import matplotlib.pyplot as plt

DATA_PATH = 'perfume_sales_data.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_data_smart(path=DATA_PATH):
    df = pd.read_csv(path)
    date_col = [col for col in df.columns if 'date' in col.lower()]
    brand_col = [col for col in df.columns if 'perfume' in col.lower() or 'brand' in col.lower()]
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    sales_col = numeric_cols[0]
    df[date_col[0]] = pd.to_datetime(df[date_col[0]])
    df.rename(columns={date_col[0]: 'ds', sales_col: 'y', brand_col[0]: 'brand'}, inplace=True)
    return df[['ds', 'y', 'brand']]

def find_best_match(name, available_brands):
    matches = get_close_matches(name, available_brands, n=1, cutoff=0.6)
    if matches:
        return matches[0]
    else:
        return None

def train_forecast_model(): # in this fnction will be training the prophet model and will be forecasting the sales for the perfume provided by the user
    data = load_data_smart()
    brands_list = data['brand'].str.lower().unique().tolist()
    perfume_to_predict = input("Enter the Perfume Name")  
    matched_brand = find_best_match(perfume_to_predict.lower(), brands_list)
    if not matched_brand:
        raise Exception(f"No close match found for product: {perfume_to_predict}")
    product_data = data[data['brand'].str.lower() == matched_brand]
    model = Prophet()
    model.fit(product_data)

    future = model.make_future_dataframe(periods=60)
    forecast = model.predict(future)
    forecast_result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(60)
    forecast_result.to_csv('/mnt/data/forecast_result.csv', index=False)
    print(f"Forecast generated and saved!")

    fig1 = model.plot(forecast)
    fig1.savefig('/mnt/data/forecast_plot.png')
    print(f"Forecast plot saved!")

with DAG(
    dag_id='perfume_sales_forecast',
    default_args=default_args,
    description='Daily perfume sales forecast using Prophet',
    schedule_interval='0 2 * * *',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['forecast', 'prophet', 'perfume'],
) as dag:

    forecast_task = PythonOperator(
        task_id='generate_sales_forecast',
        python_callable=train_forecast_model
    )

    forecast_task
