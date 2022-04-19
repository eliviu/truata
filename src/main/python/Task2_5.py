from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'random_logic': False
}

dag = DAG(
    'sample_dag',
    schedule_interval="@once",
    default_args=args
)

t1 = DummyOperator(task_id='task1', dag=dag)
t2 = DummyOperator(task_id='task2', dag=dag)
t3 = DummyOperator(task_id='task3', dag=dag)
t4 = DummyOperator(task_id='task4', dag=dag)
t5 = DummyOperator(task_id='task5', dag=dag)
t6 = DummyOperator(task_id='task6', dag=dag)

t1.set_downstream([t2, t3])
t2.set_downstream([t4, t5, t6])
t3.set_downstream([t4, t5, t6])
