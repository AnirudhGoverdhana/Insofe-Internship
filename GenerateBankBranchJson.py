import os
import logging
import json
import time
import requests
import random
# provide absoulte path of dag config json file
# E.g /home/kettle/airflow/dp_dags/dag_config.json
dag_config_file = "/home/kettle/airflow/dp_dags/dag_config.json"

no_of_errors = 0
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
json_file_path = None
data_pipeline = None
bankTables_URL = None
branchTables_URL = None

if not os.path.isfile(dag_config_file):
    logger.error("DAG Config File : {0} doesn't exist. Please check.".format(dag_config_file))
    no_of_errors += 1
else:
    with open(dag_config_file, "r") as fp:
        dag_cfg = fp.read()
    dag_cfg = json.loads(dag_cfg)
    json_file_path = dag_cfg["json_file_path"] if "json_file_path" in dag_cfg else None
    data_pipeline = dag_cfg["data_pipeline"] if "data_pipeline" in dag_cfg else None
    bankTables_URL = data_pipeline["bankTables_URL"] if "bankTables_URL" in data_pipeline and data_pipeline is not None else None
    branchTables_URL = data_pipeline["branchTables_URL"] if "branchTables_URL" in data_pipeline and data_pipeline is not None else None

if data_pipeline is None or not data_pipeline:
    logger.error("Data Pipeline not defined. Please check.".format(data_pipeline))
    no_of_errors += 1

if bankTables_URL is None or not bankTables_URL:
    logger.error("Bank tables URL not defined. Please check.".format(bankTables_URL))
    no_of_errors += 1

if branchTables_URL is None or not branchTables_URL:
    logger.error("Branch tables URL not defined. Please check.".format(branchTables_URL))
    no_of_errors += 1

if json_file_path is None or not json_file_path:
    logger.error("JSON File Path not defined. Please check.".format(json_file_path))
    no_of_errors += 1


if no_of_errors == 0:
    bank_config_json_file = os.path.join(json_file_path, "bank.json")
    result = requests.get(bankTables_URL)
    output = result.json()
    module_to_tables_list = {}
    output_dict = {"modules": []}
    for moduletotable in output["tableModelList"]:
        tables = moduletotable["targetTable"]
        module = moduletotable["moduleName"]
        if module not in module_to_tables_list:
            module_to_tables_list[module] = []
        module_to_tables_list[module].append(tables)
    for allmodules in sorted(module_to_tables_list):
        dict = {"module": allmodules, "tables": module_to_tables_list[allmodules]}
        output_dict["modules"].append(dict)
    final_bank_json = json.dumps(output_dict)
    with open(bank_config_json_file, "w") as fp:
        fp.write(final_bank_json)

    branch_config_json_file = os.path.join(json_file_path, "branch.json")
    result = requests.get(branchTables_URL)
    output = result.json()
    module_to_tables_list = {}
    output_dict = {"modules": []}
    for moduletotable in output["tableModelList"]:
        tables = moduletotable["targetTable"]
        module = moduletotable["moduleName"]
        if module not in module_to_tables_list:
            module_to_tables_list[module] = []
        module_to_tables_list[module].append(tables)
    for allmodules in sorted(module_to_tables_list):
        dict = {"module": allmodules, "tables": module_to_tables_list[allmodules]}
        output_dict["modules"].append(dict)
    final_branch_json = json.dumps(output_dict)
    with open(branch_config_json_file, "w") as fp:
        fp.write(final_branch_json)
else:
    logger.error("Errors occurred while generating bank & branch json files.")



