import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
def test_dag_function():
    logging.info("test_dag_function is running...")

test_dag = DAG(
    dag_id='test_dag',
    start_date=datetime.datetime.now())

start_operator = DummyOperator(task_id='Begin_execution', dag=test_dag)

# create_tables_in_redshift =  DummyOperator(task_id='create_tables_in_redshift',  dag=test_dag)

stage_events_to_redshift = DummyOperator(task_id='stage_events', dag=test_dag)
stage_songs_to_redshift = DummyOperator(task_id='stage_songs', dag=test_dag)

load_songplays_table = DummyOperator(
    task_id='Load_songplays_fact_table',
    dag=test_dag
)

load_user_dimension_table = DummyOperator(
    task_id='Load_user_dim_table',
    dag=test_dag
)

load_song_dimension_table = DummyOperator(
    task_id='Load_song_dim_table',
    dag=test_dag
)

load_artist_dimension_table = DummyOperator(
    task_id='Load_artist_dim_table',
    dag=test_dag
)

load_time_dimension_table = DummyOperator(
    task_id='Load_time_dim_table',
    dag=test_dag
)

run_quality_checks = DummyOperator(task_id='run_quality_checks',  dag=test_dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=test_dag)


start_operator >> [
    stage_events_to_redshift, 
    stage_songs_to_redshift
    ] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table, 
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table
] >> run_quality_checks >> end_operator
