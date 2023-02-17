#!/bin/sh

FUNC=$1

git clone https://github.com/wt358/CoOps_team2.git
mkdir py-test
cp ./CoOps_team2/airflow/airflow-DAGS/pyfile/*.py ./py-test/
# cp ./airflow-DAGS/pyfile/*.py ./py-test/

python3 -c "./py-test/copy_gpu_py.py ${FUNC}"