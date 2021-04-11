from datetime import datetime,timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

def csvtoraw(file,sep,raw,compression,**kwargs):
    ti = kwargs['ti']
    df = pd.read_csv(file,sep, low_memory=False,error_bad_lines=False)
    df.to_parquet(path=raw, compression=compression, index=False)
    ti.xcom_push('raw_path', raw)

def rawtotrusted(trusted,compression,**kwargs):
    ti = kwargs['ti']
    raw = ti.xcom_pull(task_ids='csvtoraw', key='raw_path')
    df = pd.read_parquet(raw)
    bool_dt_nasc_not_null = df['DT_NASC'].notnull()
    dt_nasc_not_null = df[bool_dt_nasc_not_null]
    dt_nasc_not_null.to_parquet(path=trusted, compression=compression, index=False)
    ti.xcom_push('trusted_path', trusted)
    print(df.head())

def trustedtorefined(refined,compression,**kwargs):
    ti = kwargs['ti']
    trusted = ti.xcom_pull(task_ids='rawtotrusted', key='trusted_path')
    df = pd.read_parquet(trusted)
    df = df[['ID_MUNICIP','NU_IDADE_N']].groupby(['ID_MUNICIP','NU_IDADE_N'])['NU_IDADE_N'] \
        .count() \
        .reset_index(name='count') \
        .sort_values(['count'], ascending=False) \
        .head(5)
    df.to_parquet(path=refined, compression=compression, index=False)

dag = DAG(
    'covid_pipeline',
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='@daily',
    catchup=True
)

task_csvtoraw = PythonOperator(
    task_id='csvtoraw',
    python_callable=csvtoraw,
    op_args=['/opt/airflow/dags/dados/INFLUD21-11-01-2021.csv',';','/opt/airflow/dags/dados/covid19/raw/covid_raw.parquet','snappy'],
    dag=dag,
)

task_rawtotrusted = PythonOperator(
    task_id='rawtotrusted',
    python_callable=rawtotrusted,
    op_args=['/opt/airflow/dags/dados/covid19/trusted/covid_trusted.parquet','snappy'],
    dag=dag,
)

task_trustedtorefined = PythonOperator(
    task_id='trustedtorefined',
    python_callable=trustedtorefined,
    op_args=['/opt/airflow/dags/dados/covid19/refined/covid_refined.parquet','snappy'],
    dag=dag,
)





task_csvtoraw >> task_rawtotrusted >> task_trustedtorefined
