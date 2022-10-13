import pandas as pd
import sqlite3
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

## Declare Variable
sqlite_connection_path = "/home/beatriz/airflow_tooltorial/data/Northwind_small.sqlite"

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]
       
    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

	
    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

def export_order_csv():
    #SQLite Connection
    sqlite_conn = sqlite3.connect(sqlite_connection_path)
    
    #SQLite SQL Order
    sql_order = "SELECT * FROM [Order]"
    output_order = pd.read_sql(sql_order, sqlite_conn)
    sqlite_conn.close()
    #Export CSV
    return output_order.to_csv('output_orders.csv', index=False)
    
def export_count_order_detail_csv():
    #SQLite Connection
    sqlite_conn = sqlite3.connect(sqlite_connection_path)
    
    #Read CSVFile
    csv_order_pd = pd.read_csv('output_orders.csv', chunksize=1000000)
    
    #Where Rio de Janeiro
    df_order = pd.concat((x.query("ShipCity == 'Rio de Janeiro'") for x in csv_order_pd), ignore_index=True)
    
    #Get OrderDetail
    sql_order_detail = "SELECT * FROM [OrderDetail]"
    df_order_detail = pd.read_sql(sql_order_detail, sqlite_conn)
    
    #Merge Order + OrderDetail
    df_merged = pd.merge(df_order_detail, df_order, left_on='OrderId', right_on='Id', how='inner')
    
    #Sum Quantity OrderDetail
    sum_value = df_merged['Quantity'].sum()
    
    #Save Count Txt
    with open('count.txt',"w") as f:
        f.write(str(sum_value))
        
    sqlite_conn.close()

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    
    task1 = PythonOperator(
        task_id='task1',
        python_callable=export_order_csv,
        provide_context=True
    )
    
    task2 = PythonOperator(
        task_id='task2',
        python_callable=export_count_order_detail_csv,
        provide_context=True
    )
    
    export_final_answer = PythonOperator(
        task_id='export_final_answer',
        python_callable=export_final_answer,
        provide_context=True
    )

task1 >> task2 >> export_final_answer