from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    start_date=datetime(2026, 9, 6),
    schedule=None,
    catchup=False,
    tags=["kubernetes"],
)
def simple_kubernetes_pod_operator_demo():

    # Task 1 – runs on Airflow worker
    @task
    def task_1_airflow_worker():
        print("Task 1: Running on Airflow worker")

    # Task 2 – runs inside a Kubernetes pod (classic operator)
    task_2_kubernetes_pod = KubernetesPodOperator(
        task_id="task_2_kubernetes_pod",
        namespace="airflow",
        image="python:3.12-slim",
        cmds=["python", "-c"],
        arguments=["print('Task 2: Running inside Kubernetes pod')"],
        name="airflow-k8s-pod-task-2",
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
        startup_timeout_seconds=600,
        # kubernetes_conn_id='kubernetes_default',
    )

    # Task 3 – runs on Airflow worker again
    @task
    def task_3_airflow_worker_again():
        print("Task 3: Running on Airflow worker again")

    # Set dependencies
    task_1_airflow_worker() >> task_2_kubernetes_pod >> task_3_airflow_worker_again()


# Instantiate the DAG
simple_kubernetes_pod_operator_demo()
