from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

dag_id = 'kubernetes-dag'

task_default_args = {
        'owner': 'coops2',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
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
env_from = [
        k8s.V1EnvFromSource(
            # configmap fields를  key-value 형태의 dict 타입으로 전달한다. 
            config_map_ref=k8s.V1ConfigMapEnvSource(name="airflow-cluster-pod-template"),
            # secret fields를  key-value 형태의 dict 타입으로 전달한다.
            secret_ref=k8s.V1SecretEnvSource(name="regcred")),
]

'''






start = DummyOperator(task_id="start", dag=dag)

run = KubernetesPodOperator(
        task_id="kubernetespodoperator",
        name="test",
        namespace='airflow-cluster',
        image='model-image.kr.ncr.ntruss.com/airflow-py:0.7',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        is_delete_operator_pod=True,
        get_logs=True,
        )

start >> run


