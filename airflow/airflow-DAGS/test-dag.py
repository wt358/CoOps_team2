"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from datetime import timedelta
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
args = {
    'owner': 'airflow',
}

task_default_args = {
        'owner': 'coops2',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
        'depends_on_past': True,
        #'execution_timeout': timedelta(minutes=5)
}


dag=DAG(
    dag_id='python_operator',
    schedule_interval=timedelta(days=7),
    default_args=task_default_args,
    start_date=days_ago(30),
    tags=['exaddsdfmple'],
    catchup=True,
) 

    # [START howto_operator_python]
def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    dag=dag,
    task_id='print_the_context',
    python_callable=print_context,
)
# [END howto_operator_python]
kubenetes_template_ex = KubernetesPodOperator(
    dag=dag,
    task_id="ex-kube-templates",
    name="ex-kube-templates",
    namespace='airflow-cluster',
    # All parameters below are able to be templated with jinja -- cmds,
    # arguments, env_vars, and config_file. For more information visit:
    # https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    # Entrypoint of the container, if not specified the Docker container's
    # entrypoint is used. The cmds parameter is templated.
    cmds=["echo"],
    # DS in jinja is the execution date as YYYY-MM-DD, this docker image
    # will echo the execution date. Arguments to the entrypoint. The docker
    # image's CMD is used if this is not provided. The arguments parameter
    # is templated.
    arguments=["{{ ds }}"],
    # The var template variable allows you to access variables defined in
    # Airflow UI. In this case we are getting the value of my_value and
    # setting the environment variable `MY_VALUE`. The pod will fail if
    # `my_value` is not set in the Airflow UI.
    # env_vars={"MY_VALUE": "{{ var.value.my_value }}"},
    # Sets the config file to a kubernetes config file specified in
    # airflow.cfg. If the configuration file does not exist or does
    # not provide validcredentials the pod will fail to launch. If not
    # specified, config_file defaults to ~/.kube/config
    # config_file="{{ conf.get('core', 'kube_config') }}",
    is_delete_operator_pod=True,
    get_logs=True,
    image="gcr.io/gcp-runtimes/ubuntu_18_0_4",
)
# [START howto_operator_python_kwargs]
def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)

# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
# for i in range(2):
#     task = PythonOperator(
#         task_id='sleep_for_' + str(i),
#         python_callable=my_sleeping_function,
#         op_kwargs={'random_base': float(i) / 10},
#         owner="coops2",
#         retries=0,
#         retry_delay=timedelta(minutes=1),
#     )

run_this >>kubenetes_template_ex
# [END howto_operator_python_kwargs]

# [START howto_operator_python_venv]
# def callable_virtualenv():
#     """
#     Example function that will be performed in a virtual environment.
#     Importing at the module level ensures that it will not attempt to import the
#     library before it is installed.
#     """
#     from time import sleep

#     from colorama import Back, Fore, Style

#     print(Fore.RED + 'some red text')
#     print(Back.GREEN + 'and with a green background')
#     print(Style.DIM + 'and in dim text')
#     print(Style.RESET_ALL)
#     for _ in range(10):
#         print(Style.DIM + 'Please wait...', flush=True)
#         sleep(10)
#     print('Finished')

# virtualenv_task = PythonVirtualenvOperator(
#     task_id="virtualenv_python",
#     python_callable=callable_virtualenv,
#     requirements=["colorama==0.4.0"],
#     system_site_packages=False,
# )
