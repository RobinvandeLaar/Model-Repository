from airflow import DAG
from airflow.api.client.local_client import Client
from airflow.models import TaskInstance
from airflow import AirflowException

import time
from datetime import datetime, timedelta

from modules.handlers.rest_handler import RestHandler
# from modules.interfaces.minio_interface import MinIOInterface
# from modules.interfaces.influx_interface import InfluxInterface

from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import logging
import requests
import json
import sys
import os

#from conf import default
default_params = {
    "metadata": {
        "user": "mmvib",
        "project": "tholen",
        "scenario": "v04-26kw",
        "experiment": "Trial_1",
        "run": "MM_workflow_run_1"
    },
    "modules": {
        "model_registry": "http://mmvib-registry:9200/registry/"
    },
    "databases": {
        "Minio": {
            "api_addr": "minio:9000",
            "db_config": {
                "secure": 'false',
                "access_key": "admin",
                "secret_key": "password"
            }
        },
        "Influx": {
            "api_addr": "influxdb:8086",
            "db_config": {
                "db_name": "energy_profiles",
                "use_ssl": 'false'
            }
        }
    },
    "tasks": {
        "TEACOS_Iteration_{}": {
            "type": "computation",
            "api_id": "TEACOS",
            "model_config": {
                "input_esdl_file_path": "test/Tholen-simple v04-26kW_output.esdl",
                "output_esdl_file_path": "test/{}/TEACOS_output.esdl"
            }
        },
        "ESSIM_Iteration_{}": {
            "type": "computation",
            "api_id": "ESSIM",
            "model_config": {
                "essim_post_body": {
                    "user": "essim",
                    "scenarioID": "essim_mmvib_adapter_test",
                    "simulationDescription": "ESSIM MMvIB adapter test",
                    "startDate": "2019-01-01T00:00:00+0100",
                    "endDate": "2019-01-01T23:00:00+0100",
                    "influxURL": "http://influxdb:8086",
                    "grafanaURL": "http://grafana:3000",
                    "natsURL": "nats://nats:4222",
                    "kpiModule": {
                        "modules": [{
                            "id": "TotalEnergyProductionID",
                            "config": {
                                "scope": "Total"
                            }
                        }]
                    }
                },
                "input_esdl_file_path": "test/{}/TEACOS_output.esdl",
                "output_esdl_file_path": "test/{}/ESSIM_output.esdl",
                "output_file_path": "test/{}/KPIs.json"
            }
        },
        "ETM_KPIs": {
            "type": "computation",
            "api_id": "ETM_KPIS",
            "model_config": {
                "input_esdl_file_path": "test/{}/ESSIM_output.esdl",
                "output_file_path": "test/ETM_KPIs.esdl",
                "scenario_ID": 2187862,
                "KPI_area": "Nederland",
                "etm_config": {
                    "path": "https://beta-esdl.energytransitionmodel.com/api/v1/",
                    "endpoint": "kpis"
                }
            }
        }
    }
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5)
}


# Initialization Space for the DAG
def subroutine_initialize(self, *args, **kwargs):

    logging.info('Initializing DAG with ' + str(kwargs['dag_run'].conf))

    task_instance = kwargs['task_instance']
    task_id = kwargs['task'].task_id.split('.')[-1]

    # Set base path XCOM for workflow
    metadata = kwargs['dag_run'].conf['metadata']

    # Check if all models are available in task list TODO: optimise conf calls
    registry = kwargs['dag_run'].conf['modules']['model_registry']

    try:
        r = requests.get(registry)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.response.text)

    # Collect unique models in our workflow
    model_inventory = set()
    task_list = kwargs['dag_run'].conf['tasks']

    for task in task_list:
        if task_list[task]['type'] == 'computation':
            model_inventory.add(task_list[task]['api_id'])

    logging.info("Model Inventory: " + str(model_inventory))

    # Check for available models
    data = r.json()
    for item in data:

        if item['name'] in model_inventory:
            model_inventory.remove(item['name'])

    # Check if we have any unavailable models left in the inventory
    if model_inventory:
        logging.info("ERROR: Not all models available in " + task_id + " , missing: " + str(model_inventory))

    return None

# Handle Loop Condition
def subroutine_loop(self, **kwargs):


    # params:
    # DAG to loop (?) -> with param set
    # loop condition

    # >>> in-place loop instead of recursion 
    # the task/handler manages the loop conditions (input/output structure)



    return None


# Generic Database Transaction Interface Task Specification
def subroutine_transaction(self, **kwargs):

    task_instance = kwargs['task_instance']
    task_id = kwargs['task'].task_id.split('.')[-1]

    logging.info('Processing Transaction: ' + str(task_id))
    logging.info('Previous Task: ' + str(next(iter(kwargs['task'].upstream_task_ids))))

    api_id = kwargs['dag_run'].conf['tasks'][task_id]['api_id']

    api_addr = kwargs['dag_run'].conf['databases'][api_id]['api_addr']
    config = kwargs['dag_run'].conf['databases'][api_id]['db_config']

    __import__(kwargs['module'])
    module = sys.modules[kwargs['module']]
    interface_ = getattr(module, kwargs['interface'])

    instance = interface_(api_addr=api_addr,
                          config=config,
                          logger=logging)
    instance.init()

    return None


# Generic Model Computation Handler Task Specification
def subroutine_computation(self, **kwargs):

    task_instance = kwargs['task_instance']
    task_id = kwargs['task'].task_id.split('.')[-1]
    model_runtime = 0

    logging.info('Run Instance: ' + str(task_instance))
    logging.info('Previous Task: ' + str(next(iter(kwargs['task'].upstream_task_ids))))
    logging.info('Task Started: ' + str(task_id))

    api_id = kwargs['dag_run'].conf['tasks'][task_id]['api_id']

    # Get API addr through registry request
    registry = kwargs['dag_run'].conf['modules']['model_registry']
    model_request = {'name': api_id}

    try:
        r = requests.post(str(registry) + 'search', json=model_request)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.response.text)

    data = r.json()
    api_addr = None

    for item in data:
        if item['status'] == "READY":
            api_addr = str(item['uri'])
            break
    if api_addr is None:
        raise AirflowException("ERROR: No model available in " + task_id)

    logging.info('Using model ' + str(api_id) + ' located at ' + str(api_addr))

    # Get experiment metadata
    metadata = kwargs['dag_run'].conf['metadata']

    # Get experiment model config
    config = kwargs['dag_run'].conf['tasks'][task_id]['model_config']

    # Initialize Model Handler
    rest_handler = RestHandler(api_addr=api_addr,
                               task_instance=task_instance,
                               metadata=metadata,
                               config=config,
                               logger=logging,
                               timeout=0) # disable timeout, as some essim simulations take longer than 60s

    # Connect Handler with Adapter
    rest_handler.get_adapter_status()
    response = rest_handler.request_model_instance()
    model_run_id = response['model_run_id']

    # Execute Model
    rest_handler.init_model_instance(model_run_id)
    response = rest_handler.run_model_instance(model_run_id)

    # Wait for Success Response
    while response['state'] != 'SUCCEEDED':
        # response = rest_handler.get_model_status(model_run_id)
        response = rest_handler.get_model_results(model_run_id)

        if response['state'] == 'ERROR':
            raise AirflowException("ERROR: Model in task " + task_id + " returned an error state")

        time.sleep(rest_handler.interval)
        model_runtime += rest_handler.interval
        if 0 < rest_handler.timeout < model_runtime:
            raise AirflowException("TIMEOUT: Task" + task_id + ": Model with address" + str(api_addr) + "and config" +
                                   str(config) + "timed out (" + str(rest_handler.timeout) + "s)")

    # Collect Model Results
    # response = rest_handler.get_model_results(model_run_id)
    logging.info("RESP:" + str(response))
    if response['result'] is not None:
        model_result = response['result']['path']
        task_instance.xcom_push(key=task_id + '_result', value=model_result)
    else:
        logging.info("ERROR: invalid model result response " + str(response))
    rest_handler.remove_model_instance(model_run_id)

    logging.info('Task Completed: ' + str(task_id))

    return None


# Terminate DAG if value passes threshold
def subroutine_finalize(self, **kwargs):
    return None

#with open('default_config.json') as json_file:
#    default_params = json.load(json_file)

# DAG Specification
with DAG('mmvib_micro_case',
          default_args=default_args,
          params=default_params,
          schedule_interval=None,
          tags=["MMvIB","ESDL","ETM","ESSIM", "TEACOS"]) as dag:


    # Task Specification
    Initialize = PythonOperator(dag=dag,
                        task_id='Initialize',
                        python_callable=subroutine_initialize,
                        op_args=['context'])

    ETM_KPIs = PythonOperator(dag=dag,
                        task_id='ETM_KPIs',
                        python_callable=subroutine_computation,
                        op_args=['context'])

    Finalize = PythonOperator(dag=dag,
                        task_id='Finalize',
                        python_callable=subroutine_finalize,
                        op_args=['context'])

    def group(number, **kwargs):
        #load the values if needed in the command you plan to execute
        dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func') }}"

        with TaskGroup(group_id='Iteration_{}'.format(number)) as tg1:
            t1 = PythonOperator(dag=dag,
                        task_id='TEACOS_Iteration_{}'.format(number),
                        python_callable=subroutine_computation,
                        op_args=['context'])

            t2 = PythonOperator(dag=dag,
                        task_id='ESSIM_Iteration_{}'.format(number),
                        python_callable=subroutine_computation,
                        op_args=['context'])

            t1 >> t2

        return tg1

    iters = 2
    prev = None
    for i in range(1,iters+1):
        item = group(i)
        Initialize >> item >> ETM_KPIs >> Finalize
        if prev is not None:
            prev >> item
            prev = item
        else:
            prev = item


