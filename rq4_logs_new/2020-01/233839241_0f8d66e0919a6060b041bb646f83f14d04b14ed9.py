from datetime import timedelta,datetime
import airflow
from airflow import DAG
from airflow import hooks, settings
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 17, 11, 20, 0, tzinfo=local_tz),
    'email': ['prakarsh2512@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 0,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def print_context(ds, **kwargs):
    # poke(ds)
    return 'Whatever you return gets printed in the logs'

dag = DAG(
    'check_steps2',
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    # schedule_interval='0 3 * * *'
    schedule_interval=timedelta(days=5)
)
step_adder=[]
step_checker=[]



def poke(run_this,t2):
    hook = S3Hook(aws_conn_id='aws_s3')
    job_flow_id = "j-2ASQREUMPJ0Y7"
    aws_conn_id = 'aws_emr'
    st=hook.read_key(key='prod_deployment/conf/athena_all_tables', bucket_name='bounce-data-platform')
    loop=st.split(",")
    print(loop)
    # X = 5 if not loop is None else len(loop)
    X=0 if loop is None else len(loop)
    for i in range(0,X):
        steps=[
            {
                'Name': 'test step',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hive',
                        '-e',
                        'msck repair table dataplatform.task_fact_daily_agg_entity'
                    ]
                }
            }
        ]
        # t3= BashOperator(
        # task_id='ag' + str(i),
        # bash_command='echo "success"',
        # dag=dag)
        # run_this>>t3>>t2
        step_addr = EmrAddStepsOperator(
            task_id='add_steps_'+str(i),
            job_flow_id="j-2ASQREUMPJ0Y7",
            aws_conn_id='aws_emr',
            steps=steps,
            dag=dag
        )

        step_checkr = EmrStepSensor(
            task_id='watch_step_'+str(i),
            job_flow_id="j-2ASQREUMPJ0Y7",
            step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
            aws_conn_id='aws_emr',
            dag=dag
        )
        run_this >> step_addr>>step_checkr>>t2


# def pk(a,b):
#     l=[]
#     for i in range(0,2) : 
#         t2= BashOperator(
#         task_id='ag{}'.format(i),
#         bash_command='echo "success"',
#         dag=dag)
#         a>>t2>>b

with dag as dag:
    run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
    )

    t2= BashOperator(
    task_id='againprint_date',
    bash_command='date',
    dag=dag)
    poke(run_this,t2)
    # run_this >> ll >> t2

    # poke(run_this,t2)





# for i in step_adder:
#     run_this>>i



