import datetime
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
	dag_id="process-employees",
	schedule_interval="0 0 * * *",
	start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
	catchup=False,
	dagrun_timeout=datetime.timedelta(minutes=60),
	)

@task
def get_data():
	# NOTE: configure this as appropriate for your airflow environment
	data_path = "PATH_TO_FOLDER"
	os.makedirs(os.path.dirname(data_path), exist_ok=True)
	url = "http://api.openweathermap.org/geo/1.0/direct?q={'Санкт-Петербург'},{7}&limit={1}&appid={4040c4e8839c60f92492ab0cd106e84a}"
	response = requests.request("GET", url)
	with open(data_path, "w") as file:
		file.write(response.text)
	postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
	conn = postgres_hook.get_conn()
	cur = conn.cursor()
	with open(data_path, "r") as file:
		cur.copy_expert(
			"COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
			file,
		)
	conn.commit()
