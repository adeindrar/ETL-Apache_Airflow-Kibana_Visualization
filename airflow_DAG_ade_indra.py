'''
=========================================================================================================

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch.

=========================================================================================================
'''
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

import psycopg2 as db
import pandas as pd

def fetch():
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.
    Fungsi diawali dengan mengkoneksikan antara database postgres ke airflow, dimana selanjutnya
    data akan dibaca dengan .read_sql dan dilakukan penyimpanan dengan .to_csv.
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)

    df=pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/data_raw.csv', index=False)
    print("-------Data Saved------")

def cleantable():
    '''
    Fungsi ini bertujuan untuk melakukan proses data cleaning dan menyimpan hasil cleaning
    ke dalam folder dags untuk selanjutnya dilakukan load file ke elasticsearch.
    '''
    df=pd.read_csv('/opt/airflow/dags/data_raw.csv')
    df.columns = df.columns.str.lower() # Ubah semua nama kolom menjadi lowercase
    df.columns = df.columns.str.strip().str.replace(' ','_') # menghilangkan whitespace
    df = df.drop_duplicates() # hapus data duplikat
    df = df.replace('[$,)(-]|', '', regex=True) # mengganti simbol yang tidak perlu
    df = df.dropna() # handle miss value
    df.to_csv('/opt/airflow/dags/data_clean.csv', index=False)

def insert_data():
    '''
    Fungsi ini bertujuan untuk memasukkan data yang sudah di cleaning pada fungsi sebelumnya
    ke elasticsearch untuk selanjutnya dilakukan data visualisasi, analisis, dll di kibana.
    '''
    es = Elasticsearch("elasticsearch")
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="milestone3", doc_type="doc", body=doc, id=i+1)
        print(res)


default_args= {
    'owner': 'indra',
    'start_date': datetime(2024, 11, 1, 9, 0, 0) - timedelta(hours=7)
}

with DAG( 
    "M3",
    description='EDA employee survey',
    default_args=default_args,
    schedule_interval='10-30/10 9 * * 6', 
    catchup=False) as dag:


    # task: 1 fetch_from_postgresql
    fetching_data = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch
    )
    
    # task: 2 data_cleaning
    cleandata = PythonOperator(
        task_id='cleaning',
        python_callable=cleantable
    )

    # task: 3 post_to_elasticsearch
    post_to_es = PythonOperator(
        task_id= 'insert_data',
        python_callable=insert_data
    )



    fetching_data >> cleandata >> post_to_es