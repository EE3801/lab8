import datetime as dt
from datetime import timedelta
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg as db
from elasticsearch import Elasticsearch

def queryPostgresql():

    # replace localhost with host.docker.internal
    conn_string="dbname='carpark_system' host='ec2-47-129-189-190.ap-southeast-1.compute.amazonaws.com' user='airflow' password='airflow'"

    conn=db.connect(conn_string)

    df=pd.read_sql('select * from public."CarPark"',conn)

    df.to_csv('/opt/airflow/dags/data/carpark_system.csv')

    print("-------Data Saved------")

def insertElasticsearch():

    # replace localhost with host.docker.internal
    es = Elasticsearch({'https://ec2-47-129-189-190.ap-southeast-1.compute.amazonaws.com:9200'}, basic_auth=("elastic", "L_P5_ympxlk0IUZ3AH3C"), verify_certs=False) 

    df=pd.read_csv('/opt/airflow/dags/data/carpark_system.csv')

    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="frompostgresql",
                    # doc_type="doc",
                    id=i,
                    document=doc) # replaced body with document
        print(res)

default_args = {
    'owner': 'gmscher',
    'start_date': dt.datetime(2025, 7, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('carpark_system_readfrompostgresql_toelasticsearch_DBdag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      
                           # '0 * * * *',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
         python_callable=queryPostgresql)

    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                python_callable=insertElasticsearch)

getData >> insertData

