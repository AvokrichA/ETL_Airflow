import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from datatime import datatime

dag =  DAG('my_dag_6_1',
           schedule_interval='0 12 * * *', 
           start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
           catchup=False)

gen_bash = BashOperator(
    task_id= gen_bash, 
    bash_command = 'echo $RANDOM',
    dag=dag,
)
gen_bash
