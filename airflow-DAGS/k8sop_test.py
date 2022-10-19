from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (KubernetesPodOperator,)

dag_id = 'kubernetes-dag'

task_default_args = {
        'owner': 'coops2',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': True,
        'execution_timeout': timedelta(hours=1)
}

dag = DAG(
        dag_id=dag_id,
        description='kubernetes pod operator',
        start_date=days_ago(2),
        default_args=task_default_args,
        schedule_interval=timedelta(days=1),
        max_active_runs=1
)
'''
env = Secret(
        'env',
        'TEST',
        'test_env',
        'TEST',
)

pod_resources = Resources()
pod_resources.request_cpu = '1000m'
pod_resources.request_memory = '2048Mi'
pod_resources.limit_cpu = '2000m'
pod_resources.limit_memory = '4096Mi'


configmaps = [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='secret')),
        ]
'''

start = DummyOperator(task_id="start", dag=dag)

run = KubernetesPodOperator(
        task_id="kubernetespodoperator",
        namespace='airflow-cluster',
        image='model-image.kr.ncr.ntruss.com/airflow-py',
        name="job",
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
        )

start >> run


