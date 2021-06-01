import os
import logging
import json
import time
import requests
import random


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from DBInterface import updateDAG_DB,checkPreviousRunForDAG_DB,getBankRunStatus_DB
from Commons import copySourceToTarget_CM,checkTableJobStatus_CM,getTablesListToRun_CM,validateInput_CM



default_args = {
    'owner': 'fincluez_etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(0)
}

# provide absoulte path of dag config json file
dag_config_file = "/home/kettle/airflow/dp_dags/DagConfig.json"

# setting branch to "branch_code" for branch DAG -- pass appropriate BRANCH_CODE value
branch = "000"

no_of_errors = 0
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
json_file_path = None
postgres_info = None
data_pipeline = None
branch_config_json_file = None
source_tables_list = []

if not os.path.isfile(dag_config_file):
    logger.error("DAG Config File : {0} doesn't exist. Please check.".format(dag_config_file))
    no_of_errors += 1
else:
    with open(dag_config_file, "r") as fp:
        dag_cfg = fp.read()
    dag_cfg = json.loads(dag_cfg)
    json_file_path = dag_cfg["json_file_path"] if "json_file_path" in dag_cfg else None
    postgres_info = dag_cfg["postgres_info"] if "postgres_info" in dag_cfg else None
    data_pipeline = dag_cfg["data_pipeline"] if "data_pipeline" in dag_cfg else None
   
if json_file_path is None or not json_file_path:
    logger.error("JSON File Path not defined. Please check.".format(json_file_path))
    no_of_errors += 1
else:
    branch_config_json_file = os.path.join(json_file_path, "branch.json")
    if not os.path.exists(branch_config_json_file):
        logger.error("branch configuration json file : {0} not found".format(branch_config_json_file))
        no_of_errors += 1
    else:
        with open(branch_config_json_file, "r") as fp:
            config = fp.read()
        modules_list = json.loads(config)["modules"]
        for moduletotable in modules_list:
            module = moduletotable["module"]
            tables = moduletotable["tables"]
            source_tables_list.extend(tables)
            
if postgres_info is None or not postgres_info:
    logger.error("Postgres info not defined. Please check.".format(postgres_info))
    no_of_errors += 1
    
if data_pipeline is None or not data_pipeline:
    logger.error("DataPipeline info not defined. Please check.".format(data_pipeline))
    no_of_errors += 1

def checkBankRunStatus_DB(**kwargs):
    return getBankRunStatus_DB(**kwargs)

if no_of_errors == 0:
    dag_id = "BRANCH_" + branch
    dag = DAG(
                dag_id,
                schedule_interval=None,
                catchup=False,
                default_args=default_args
             )
    kickOffDag = DummyOperator(task_id='START', dag=dag)
    validateInput = PythonOperator(
        task_id='VALIDATE_INPUT',
        op_kwargs={
            "logging": logger
        },
        python_callable=validateInput_CM,
        provide_context=True,
        dag=dag
    )
    checkPreviousRunForBankDAG = BranchPythonOperator(
        task_id='CHECK_LAST_RUN',
        python_callable=checkPreviousRunForDAG_DB,
        op_kwargs={
            "logging": logger,
            "postgres": postgres_info,
            "data_pipeline": data_pipeline
            
        },
        provide_context=True,
        dag=dag
    )
    # check branch dag  - if finished proceed, else wait
    checkBankRunStatus = PythonOperator(
        task_id='CHECK_BANK',
        python_callable=checkBankRunStatus_DB,
        op_kwargs={
            "logging": logger,
            "postgres": postgres_info
        },
        provide_context=True,
        retries=10,
        retry_delay=timedelta(minutes=1),
        dag=dag
    )
    moduleNode = BranchPythonOperator(
        task_id='MODULES',
        python_callable=lambda: "MODULES",
        dag=dag
    )
    
    updateBranchDAG = PythonOperator(
        task_id='UPDATE_BRANCH_STATUS',
        python_callable=updateDAG_DB,
        op_kwargs={"branch_code": '{0}'.format(branch),
                   'tid': "UPDATE_BRANCH_STATUS",
                   "logging": logger,
                   "postgres": postgres_info,
                   "data_pipeline": data_pipeline,
                   "source_tables_list": source_tables_list
                   },
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=random.randint(2 * 60, 5 * 60)),
        dag=dag
    )
    
    success = DummyOperator(task_id='SUCCESS', dag=dag)
    fail_notrun = DummyOperator(task_id='FAIL_OR_NOTRUN', dag=dag)
    kickOffDag >> validateInput >> checkPreviousRunForBankDAG >> [success, fail_notrun]
    success >> updateBranchDAG
    fail_notrun >>  checkBankRunStatus >> moduleNode
    
    
    with open(branch_config_json_file, "r") as fp:
        config = fp.read()
    modules_list = json.loads(config)["modules"]
    for moduletotable in modules_list:
        input_table_list = []
        module = moduletotable["module"]
        tables = moduletotable["tables"]
        input_table_list.extend(tables)
   

        airflowModule = BranchPythonOperator(
            task_id=module,
            python_callable=getTablesListToRun_CM,
            op_kwargs={"module": module,
                       "input_table_list":input_table_list,
                       "logging": logger,
                       "postgres": postgres_info,
                       "data_pipeline": data_pipeline
                       },
            provide_context=True,
            dag=dag
        )
        moduleNode >> airflowModule
		
        for table in input_table_list:
            airflowTable = PythonOperator(task_id=table,
                               python_callable=copySourceToTarget_CM,
                               op_kwargs={"table_name": table,
                                          "module": module,
                                          "branch_code": '{0}'.format(branch),
                                          "logging": logger,
                                          "data_pipeline": data_pipeline
                                          },
                               provide_context=True,
                               retries=10,
                               retry_delay=timedelta(seconds=random.randint(2*60, 5*60)),
                               dag=dag
                               )
            airflowModule >> airflowTable >> updateBranchDAG
       
else:
    logger.error("Cannot start DAG due to errors.")
