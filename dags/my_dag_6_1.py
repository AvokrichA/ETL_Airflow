import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


dag =  DAG('my_dag_6_1',
           schedule_interval=None, 
           start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
           catchup=False)

gen_bash = BashOperator(
    task_id= 'gen_bash', 
    bash_command = 'echo $RANDOM',
    dag=dag,
)
gen_bash
