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
        #'execution_timeout': timedelta(minutes=5)
}

dag = DAG(
        dag_id=dag_id,
        description='kubernetes pod operator',
        start_date=days_ago(2),
        default_args=task_default_args,
        schedule_interval=timedelta(days=7),
        max_active_runs=3,
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
pod_resources = Resources()
pod_resources.limit_gpu = '1'





start = DummyOperator(task_id="start", dag=dag)

run_iqr = KubernetesPodOperator(
        task_id="iqr_gan_pod_operator",
        name="iqr-gan",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.6',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["python3" ],
        arguments=["gpu_py.py", "iqr"],
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'kubernetes.io/hostname',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            'values': [
                                'gpu-node-w-1ouo',
                                #'pool-1',
                                ]
                            }]
                        }]
                    }
                }
            },
        resources=pod_resources,
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )

run_lstm = KubernetesPodOperator(
        task_id="lstm_pod_operator",
        name="lstm-auto-encoder",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.6',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["python3" ],
        arguments=["gpu_py.py", "lstm"],
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'kubernetes.io/hostname',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            'values': [
                                'gpu-node-w-1ouo',
                                #'pool-1',
                                ]
                            }]
                        }]
                    }
                }
            },
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )
run_svm = KubernetesPodOperator(
        task_id="oc_svm_pod_operator",
        name="oc-svm",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.6',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["python3","gpu_py.py" ],
        arguments=["oc_svm"],
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'kubernetes.io/hostname',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            'values': [
                                'high-memory-w-1ih9',
                                'high-memory-w-1iha',
                                #'pool-1',
                                ]
                            }]
                        }]
                    }
                }
            },
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )
after_aug = DummyOperator(task_id="Aug_fin", dag=dag)
start >> run_iqr >> after_aug 
after_aug >> [run_svm, run_lstm]


