from prefect import flow,task
from prefect_dask.task_runners import DaskTaskRunner


@task
def hello_dask():
    print("Hello from Dask!")

@flow(task_runner=DaskTaskRunner(address="tcp://localhost:8786"))
def subflow():
    hello_dask.submit()


@flow(log_prints=True)
def buy():
    print("Buying securities")
    subflow()


if __name__ == "__main__":
    buy.from_source(
        source="https://github.com/jsshizhan/demo.git", 
        entrypoint="test1.py:buy"
    ).deploy(name="deployment",work_pool_name="process_pool")