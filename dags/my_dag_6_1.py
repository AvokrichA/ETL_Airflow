from airflow import DAG
from airflow.operators.bash import BushOperator, PythonOperator
from datatime import datatime

def gen():
    return random.randrange(0, 10)

dag =  DAG('my_dag_6_1', start_date=datatime(2023,1,1), schedule_interval='0 12 * * *',
          catchup=False)

gen_operator = PythonOperator(task_id=gen, dag=dag)
