from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_AIRFLOW_ARGS={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def run():
    # Open pipeline
    with DAG(
        "Tutorial",
        default_args=DEFAULT_AIRFLOW_ARGS,
        description="A simple tutorial of DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"]
    ) as dag:
        # Define tasks
        task1 = BashOperator(task_id="print_date", bash_command="date")
        task2 = BashOperator(
            task_id="sleep",
            depends_on_past=False,
            bash_command="sleep 5",
            retries=3
        )

        # Add task document
        task1.doc_md = dedent(
            """\
            #### Task Documentation
            You can document your task using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page.
            ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

            """
        )
        
        dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
        dag.doc_md = """
            This is a documentation placed anywhere
        """    # otherwise, type it like this
        task3 = BashOperator(
            task_id="templated",
            depends_on_past=False,
            bash_command=dedent(
                """
                {% for i in range(5) %}
                    echo "{{ ds }}"
                    echo "{{ macros.ds_add(ds, 7)}}"
                {% endfor %}
                """
            )
        )

        # Setup dependency between tasks (using bit-shift operator)
        task1 >> [task2, task3]
        # Alternatively,
        # task1.set_downstream([task2, task3])
        # task2.set_upstream(task1)
        # task3.set_upstream(task1) 


if __name__ == "__main__":
    run()
