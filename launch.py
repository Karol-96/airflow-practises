import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



#Instantiating a DAG
dag = DAG(dag_id='download_dag_launches',
          start_date = datetime.now() - timedelta(days=14),
          schedule_interval=None)


#Bash command to download data with curl
download_launches = BashOperator(
    task_id='download_launches',
    bash_command = "curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag = dag,
) 

def _get_pictures():
    #Ensuring or Checking that the path exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        print(launches["results"])
        image_urls = [launch["image"] for launch in launches["results"]] 
        for image_url in image_urls:
            try:
                response = requests.get(image_url) 
                image_filename = image_url.split("/")[-1] 
                target_file = f"/tmp/images/{image_filename}" 
                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

def _get_pictures():
    #Ensuring or Checking that the path exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]] 
        for image_url in image_urls:
            try:
                response = requests.get(image_url) 
                if response.status_code == 200: # Check if response is valid
                    image_filename = image_url.split("/")[-1] 
                    target_file = f"/tmp/images/{image_filename}" 
                    with open(target_file, "wb") as f:
                        f.write(response.content)
                        print(f"Downloaded {image_url} to {target_file}")
                else:
                    print(f"Could not download {image_url}: {response.status_code}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator( task_id="get_pictures", python_callable=_get_pictures, dag=dag,)

notify = BashOperator(
task_id="notify",
bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."', dag=dag,)

download_launches >> get_pictures >> notify

