from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os


@dag(
    start_date=datetime(2022, 10, 10),
    schedule=None,
    catchup=False,
)
def py_virtual_env():
    test1 = BashOperator(
        task_id="test1",
        bash_command="echo $ASTRO_PYENV_my_venv && pwd",
    )

    test2 = BashOperator(
        task_id="test2",
        bash_command="cat $ASTRO_PYENV_my_venv",
    )

    test3 = BashOperator(
        task_id="test3",
        bash_command="cd /home/astro/.venv/my_venv/bin/ && ls -la",
    )

    test4 = BashOperator(
        task_id="test4",
        bash_command="/home/astro/.venv/my_venv/bin/python include/test.py",
        cwd="/usr/local/airflow",
    )

    @task.external_python(
        task_id="external_python",
        python=os.environ["ASTRO_PYENV_my_venv"],
        expect_airflow=False,
    )
    def callable_external_python():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.snowpark import Session

        print("Hello")
        print(SnowflakeHook.__module__)
        print(Session.__module__)

    task_external_python = callable_external_python()

    test1 >> test2 >> test3 >> test4 >> task_external_python


py_virtual_env()
