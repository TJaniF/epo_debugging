# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:9.0.0

COPY ./venv-requirements.txt ./venv-requirements.txt

PYENV 3.8 my_venv ./venv-requirements.txt