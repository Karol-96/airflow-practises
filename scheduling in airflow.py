#Initial (unscheduled) version of the event DAG (dags/01_unscheduled.py)
import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
   dag_id="01_unscheduled",
   start_date=dt.datetime(2019, 1, 1),
   schedule_interval=None,
)


fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events.json " "https://localhost:5000/events"),
        dag=dag, )


def _calculate_stats(input_path, output_path):
        """Calculates event statistics."""
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index()
        Path(output_path).parent.mkdir(exist_ok=True)
        stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
                task_id="calculate_stats",
                python_callable=_calculate_stats,
                op_kwargs={
                "input_path": "/data/events.json",
                "output_path": "/data/stats.csv",},
                dag=dag, )


fetch_events >> calculate_stats

#Defining Scheduled Version for above:
dag = DAG(
dag_id="02_daily_schedule", schedule_interval="@daily", start_date=dt.datetime(2019, 1, 1), ...
)

#Defining End Date
dag = DAG(
dag_id="03_with_end_date", schedule_interval="@daily", start_date=dt.datetime(year=2019, month=1, day=1), end_date=dt.datetime(year=2019, month=1, day=5),
)
#Defining Frequency based interval
dag = DAG(
dag_id="04_time_delta", schedule_interval=dt.timedelta(days=3), start_date=dt.datetime(year=2019, month=1, day=1), end_date=dt.datetime(year=2019, month=1, day=5),
)

#Templating Dates jinja templating
fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events.json " "http://localhost:5000/events?" "start_date={{execution_date.strftime('%Y-%m-%d')}}" "&end_date={{next_execution_date.strftime('%Y-%m-%d')}}"
        ),
        dag=dag
        )

#Using Templating Shortcuts
#ds provides YYYY- MM-DD formatted execution_date.
fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "http://localhost:5000/events?" "start_date={{ds}}&"
        "end_date={{next_ds}}" ),
        dag=dag, )

#Writing event data to separate files per date (dags/08_templated_path.py)
fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/{{ds}}.json " "http://localhost:5000/events?" "start_date={{ds}}&" "end_date={{next_ds}}"),
        dag=dag,)


#Taking dates from dictionary
def _calculate_stats(**context):
        """Calculates event statistics."""
        input_path = context["templates_dict"]["input_path"] 
        output_path = context["templates_dict"]["output_path"]
        Path(output_path).parent.mkdir(exist_ok=True)
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index() 
        stats.to_csv(output_path, index=False)
        calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        templates_dict={
        "input_path": "/data/events/{{ds}}.json",
        "output_path": "/data/stats/{{ds}}.csv",
        },
        dag=dag, )


#Two jobs in one task, to break atomicity (dags/10_non_atomic_send.py)
def _calculate_stats(**context):
        """Calculates event statistics."""
        input_path = context["templates_dict"]["input_path"] output_path = context["templates_dict"]["output_path"]
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index() stats.to_csv(output_path, index=False)
        email_stats(stats, email="user@example.com")



#Splitting into multiple tasks to improve atomicity (dags/11_atomic_send.py)
def _send_stats(email, **context):
        stats = pd.read_csv(context["templates_dict"]["stats_path"]) email_stats(stats, email=email)
        send_stats = PythonOperator(
        task_id="send_stats",
        python_callable=_send_stats,
        op_kwargs={"email": "user@example.com"}, templates_dict={"stats_path": "/data/stats/{{ds}}.csv"}, dag=dag,
        )
calculate_stats >> send_stats

#Existing implementation for fetching events (dags/08_templated_paths.py)
fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
"mkdir -p /data/events && "
"curl -o /data/events/{{ds}}.json " "http://localhost:5000/events?" "start_date={{ds}}&" "end_date={{next_ds}}"
),
dag=dag, )