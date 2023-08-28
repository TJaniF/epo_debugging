# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:9.0.0

PYENV 3.8 my_venv ./snowpark-requirements.txt
PYENV 3.8 my_seaborn_venv ./seaborn-requirements.txt