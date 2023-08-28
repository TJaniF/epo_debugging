from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os


@dag(
    start_date=datetime(2022, 10, 10),
    schedule=None,
    catchup=False,
)
def seaborn_venv_works():
    test1 = BashOperator(
        task_id="test1",
        bash_command="echo $ASTRO_PYENV_my_seaborn_venv && pwd",
    )

    test2 = BashOperator(
        task_id="test2",
        bash_command="cat $ASTRO_PYENV_my_venv",
    )

    test3 = BashOperator(
        task_id="test3",
        bash_command="cd /home/astro/.venv/my_seaborn_venv/bin/ && ls -la",
    )

    test4 = BashOperator(
        task_id="test4",
        bash_command="/home/astro/.venv/my_seaborn_venv/bin/python include/test_seaborn.py",
        cwd="/usr/local/airflow",
    )

    @task.external_python(
        task_id="external_python",
        python=os.environ["ASTRO_PYENV_my_seaborn_venv"],
        expect_airflow=False,
    )
    def callable_external_python():
        import seaborn

        print("Hello")
        print(seaborn.__version__)

    task_external_python = callable_external_python()

    test1 >> test2 >> test3 >> [test4, task_external_python]


seaborn_venv_works()
