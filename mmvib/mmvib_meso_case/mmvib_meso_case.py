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

default_params = {
    "metadata": {
        "experiment": "Trial_1_Meso",
        "project": "meso",
        "run": "MM_workflow_run_1",
        "scenario": "Tholen",
        "user": "mmvib"
    },
    "modules": {
        "model_registry": "http://mmvib-registry:9200/registry/"
    },
    "databases": {
        "Influx": {
            "api_addr": "influxdb:8086",
            "db_config": {
                "db_name": "energy_profiles",
                "use_ssl": "false"
            }
        },
        "Minio": {
            "api_addr": "minio:9000",
            "db_config": {
                "access_key": "admin",
                "secret_key": "password",
                "secure": "false"
            }
        }
    },
    "tasks": {
        "CTM_ETM_Iteration_1": {
            "api_id": "CTM",
            "model_config": {
                "base_path": "meso",
                "ctm_config": {
                    "CTM_scenario_ID": "base",
                    "ETM_scenario_ID": "13579",
                    "endpoint": "https://carbontransitionmodel.com/api/"
                },
                "input_esdl_file_path": "meso/output_file_3.esdl",
                "output_esdl_file_path": "meso/output_file_4.esdl"
            },
            "type": "computation"
        },
        "CTM_ETM_Iteration_2": {
            "api_id": "CTM",
            "model_config": {
                "base_path": "meso",
                "ctm_config": {
                    "CTM_scenario_ID": "base",
                    "ETM_scenario_ID": "13579",
                    "endpoint": "https://carbontransitionmodel.com/api/"
                },
                "input_esdl_file_path": "meso/output_file_5.esdl",
                "output_esdl_file_path": "meso/output_file_6.esdl"
            },
            "type": "computation"
        },
        "CTM_Model": {
            "api_id": "CTM",
            "model_config": {
                "ctm_config": {
                    "CTM_scenario_ID": "base",
                    "ETM_scenario_ID": "13579",
                    "endpoint": "https://carbontransitionmodel.com/api/"
                },
                "input_esdl_file_path": "meso/Tholen-simple v04-26kW_output.esdl",
                "output_esdl_file_path": "meso/output_file_1.esdl"
            },
            "type": "computation"
        },
        "ETM_Model": {
            "api_id": "ETM_KPIS",
            "model_config": {
                "KPI_area": "Nederland",
                "etm_config": {
                    "endpoint": "kpis",
                    "path": "https://beta-esdl.energytransitionmodel.com/api/v1/"
                },
                "input_esdl_file_path": "meso/output_file_1.esdl",
                "output_file_path": "meso/output_file_2.esdl",
                "scenario_ID": 13579
            },
            "type": "computation"
        },
        "TEACOS_Iteration_1": {
            "api_id": "TEACOS",
            "model_config": {
                "input_esdl_file_path": "meso/output_file_2.esdl",
                "output_esdl_file_path": "meso/output_file_3.esdl"
            },
            "type": "computation"
        },
        "TEACOS_Iteration_2": {
            "api_id": "TEACOS",
            "model_config": {
                "input_esdl_file_path": "meso/output_file_4.esdl",
                "output_esdl_file_path": "meso/output_file_5.esdl"
            },
            "type": "computation"
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
        response = rest_handler.get_model_results(model_run_id)

        if response['state'] == 'ERROR':
            raise AirflowException("ERROR: Model in task " + task_id + " returned an error state")

        time.sleep(rest_handler.interval)
        model_runtime += rest_handler.interval
        if 0 < rest_handler.timeout < model_runtime:
            raise AirflowException("TIMEOUT: Task" + task_id + ": Model with address" + str(api_addr) + "and config" +
                                   str(config) + "timed out (" + str(rest_handler.timeout) + "s)")

    # Collect Model Results
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

# DAG Specification
with DAG('mmvib_meso_case',
          default_args=default_args,
          params=default_params,
          schedule_interval=None,
          tags=["MMvIB","ESDL","CTM","ETM", "TEACOS"]) as dag:


    # Task Specification
    Initialize = PythonOperator(dag=dag,
                        task_id='Initialize',
                        python_callable=subroutine_initialize,
                        op_args=['context'])

    CTM_Model = PythonOperator(dag=dag,
                        task_id='CTM_Model',
                        python_callable=subroutine_computation,
                        op_args=['context'])

    ETM_Model = PythonOperator(dag=dag,
                        task_id='ETM_Model',
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
                        task_id='CTM_ETM_Iteration_{}'.format(number),
                        python_callable=subroutine_computation,
                        op_args=['context'])

            t1 >> t2

        return tg1

    iters = 2
    prev = None
    for i in range(1,iters+1):
        item = group(i)
        Initialize >> CTM_Model >> ETM_Model >> item >> Finalize
        if prev is not None:
            prev >> item
            prev = item
        else:
            prev = item


