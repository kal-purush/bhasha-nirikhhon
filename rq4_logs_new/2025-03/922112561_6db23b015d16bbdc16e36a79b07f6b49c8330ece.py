from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import subprocess
import logging

import os

# /opt/airflow/ 위치에서 실행
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

log_dir = os.path.join(BASE_DIR, "logs/")
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'dag.log')
# 기존 핸들러에 추가하거나 기본 설정 재구성
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def run_script(script_name):
    script_path = os.path.join(BASE_DIR, script_name)  # 절대 경로 변환
    logging.info(f"Executing script: {script_path}")
    logging.info(f"Current working directory: {os.getcwd()}")
    try:
        result = subprocess.run(["python", script_path], check=True, capture_output=True, text=True)
        logging.info(f"Script output: {result.stdout}")
        logging.info(f"Script error (if any): {result.stderr}")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Script execution failed: {e}")
        logging.error(f"Output: {e.output}")
        logging.error(f"Error: {e.stderr}")

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 27),
    "retries": 1,
}

dag = DAG(
    dag_id="daily_crawling_and_upload",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # 매일 자정 실행
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=4,  # DAG의 동시 실행 인스턴스 제한
)


# 크롤링 태스크 병렬 실행 (TaskGroup 활용)
with TaskGroup("crawling_tasks", dag=dag) as crawling_tasks:
    crawl_scripts = [
        os.path.join(BASE_DIR, "crawling/employment_detail/jasoseol.py"),
        os.path.join(BASE_DIR, "crawling/employment_detail/linkareer.py"),
        os.path.join(BASE_DIR, "crawling/employment_detail/wanted.py"),
        os.path.join(BASE_DIR, "crawling/employment_detail/zighang.py")
    ]

    try:
        for idx, script in enumerate(crawl_scripts):
            logging.info(f"{script} 크롤링 시작")
            basename = os.path.basename(script).strip()
            name_without_ext = basename.split('.')[0] if basename else ""
            task_id = f"crawl_{name_without_ext}"
            
            PythonOperator(
                task_id=task_id,
                python_callable=run_script,
                op_kwargs={"script_name": script},
                dag=dag,
            )
    except Exception as e:
        logging.error(e)

# MinIO 업로드 태스크 (크롤링 완료 후 실행)
minio_upload = PythonOperator(
    task_id="upload_to_minio",
    python_callable=run_script,
    op_kwargs={
        "script_name": os.path.join(BASE_DIR, "setting_object_storage/stand-alone/minio_upload.py"),
        "extra_args": [
            "--bucket_name", "job-data",
            "--directory_path", os.path.join(BASE_DIR, "crawling/results")
        ]
    },
    dag=dag,
)

# DAG 실행 순서
crawling_tasks >> minio_upload