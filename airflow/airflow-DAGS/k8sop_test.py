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

configmaps = [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-cluster-pod-template'))
        ]
'''
pod_resources = Resources()
pod_resources.limit_gpu = '1'
'''

secret_env = Secret(
        # Expose the secret as environment variable.
        deploy_type='env',
        # The name of the environment variable, since deploy_type is `env` rather
        # than `volume`.
        deploy_target='MONGO_URL_SECRET',
        # Name of the Kubernetes Secret
        secret='db-secret-ggd7k5tgg2',
        # Key of a secret stored in this Secret object
        key='MONGO_URL_SECRET')
secret_volume = Secret(
        deploy_type='volume',
        # Path where we mount the secret as volume
        deploy_target='/var/secrets/db',
        # Name of Kubernetes Secret
        secret='db-secret-ggd7k5tgg2',
        # Key in the form of service account file name
        key='mongo-url-secret.json')
secret_all = Secret('env', None, 'db-secret-ggd7k5tgg2')
secret_all1 = Secret('env', None, 'airflow-cluster-config-envs')
secret_all2 = Secret('env', None, 'airflow-cluster-db-migrations')
secret_all3 = Secret('env', None, 'airflow-cluster-pgbouncer')
secret_all4 = Secret('env', None, 'airflow-cluster-pgbouncer-certs')
secret_all5 = Secret('env', None, 'airflow-cluster-postgresql')
secret_all6 = Secret('env', None, 'airflow-cluster-sync-users')
secret_all7 = Secret('env', None, 'airflow-cluster-token-8qgp2')
secret_all8 = Secret('env', None, 'airflow-cluster-webserver-config')
secret_all9 = Secret('env', None, 'airflow-git-ssh-secret2')
secret_alla = Secret('env', None, 'airflow-ssh-git-secret')
secret_allb = Secret('env', None, 'default-token-hkdgr')



gpu_aff={
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
                            'pseudo-gpu-w-1yz7',
                            #'pool-1',
                            ]
                        }]
                    }]
                }
            }
        }
cpu_aff={
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'kubernetes.io/hostname',
                        'operator': 'In',
                        'values': [
                            'high-memory-w-1ih9',
                            'high-memory-w-1iha',
                            ]
                        }]
                    }]
                }
            }
        }


start = DummyOperator(task_id="start", dag=dag)

run_iqr = KubernetesPodOperator(
        task_id="iqr_gan_pod_operator",
        name="iqr-gan",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.15',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["python3" ],
        arguments=["gpu_py.py", "iqr"],
        affinity=gpu_aff,
        #resources=pod_resources,
        secrets=[secret_all,secret_all1 ,secret_all2 ,secret_all3, secret_all4, secret_all5, secret_all6, secret_all7, secret_all8, secret_all9, secret_alla, secret_allb ],
        #secrets=[secret_volume,secret_all],
        #env_vars={'MONGO_URL_SECRET':'{{var.value.MONGO_URL_SECRET}}'},
        #configmaps=configmaps,
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )

run_lstm = KubernetesPodOperator(
        task_id="lstm_pod_operator",
        name="lstm-auto-encoder",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.15',
        image_pull_policy="Always",
        #image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["python3" ],
        arguments=["gpu_py.py", "lstm"],
        #affinity=gpu_aff,
        #resources=pod_resources,
        secrets=[secret_all,secret_all1 ,secret_all2 ,secret_all3, secret_all4, secret_all5, secret_all6, secret_all7, secret_all8, secret_all9, secret_alla, secret_allb ],
        #env_vars={'MONGO_URL_SECRET':'/var/secrets/db/mongo-url-secret.json'},
        #configmaps=configmaps,
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )
run_svm = KubernetesPodOperator(
        task_id="oc_svm_pod_operator",
        name="oc-svm",
        namespace='airflow-cluster',
        image='wcu5i9i6.kr.private-ncr.ntruss.com/cuda:0.15',
        image_pull_policy="Always",
        image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        secrets=[secret_all,secret_all1 ,secret_all2 ,secret_all3, secret_all4, secret_all5, secret_all6, secret_all7, secret_all8, secret_all9, secret_alla, secret_allb ],
        cmds=["python3","gpu_py.py" ],
        arguments=["oc_svm"],
        affinity=cpu_aff,
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        )
after_aug = DummyOperator(task_id="Aug_fin", dag=dag)
after_ml = DummyOperator(task_id="ML_fin", dag=dag)
start >> run_iqr >> after_aug 
after_aug >> [run_svm, run_lstm] >> after_ml


