import os
import requests
import random
import time

from datetime import datetime

import DBInterface


def getTablesListToRun_CM(**kwargs):
    logger = kwargs["logging"]
    logger.info("In func : getTablesListToRun")
    input_module = kwargs["module"]
    input_table_list = kwargs["input_table_list"]
    for table in input_table_list:
        logger.info("Checking table : {} for run".format(table))
        kwargs["table_name"] = table
        DBInterface.checkPreviousRunForTable_DB(**kwargs)
    return input_module


def copySourceToTarget_CM(**kwargs):
    logger = kwargs["logging"]
    logger.info("In func : copySourceToTarget_CM")
    table_name = kwargs["table_name"]
    input_module = kwargs["module"]
    branch = kwargs["branch_code"]
    data_pipeline = kwargs["data_pipeline"]
    el_run_date = kwargs["dag_run"].conf.get("elRunDate")
    entity_code = kwargs["dag_run"].conf.get("entityCode")
    task_instance = kwargs["task_instance"]
    table_run_flag = task_instance.xcom_pull(task_ids="{}".format(input_module), key="{}".format(table_name + "_RUN_FLAG"))
    logger.info(
        "table_name : {} input_module : {} el_run_date : {} "
        "entity_code : {} task_instance : {} data_pipeline : {}".format(
            table_name,
            input_module,
            el_run_date,
            entity_code,
            task_instance,
            data_pipeline,
        )
    )
    logger.info("table_run_flag : {}".format(table_run_flag))
    copyTable = "FAIL"
    if table_run_flag:
        copyTable_URL = os.path.join(kwargs["data_pipeline"]["copyTable_URL"])
        json_params = {}
        json_params.update({"elRunDate": el_run_date})
        json_params.update({"entityCode": entity_code})
        json_params.update({"branchCode": branch})
        json_params.update({"sourceTableName": table_name})
        logger.info(
            "json_params : {}".format(json_params)
        )
        resultCopyTable = requests.post(copyTable_URL, json=json_params)
        
        resultCopyTable = resultCopyTable.json()
        logger.info("Response resultCopyTable : {}".format(resultCopyTable))
        responseStatus = resultCopyTable["responseStatus"]
        if responseStatus == "SUCCESS":
            jobId = resultCopyTable["jobId"]
            kwargs["jobId"] = jobId
            checkTableJobStatus_CM(**kwargs)
        else:
            raise Exception(
                "EL for Table : {} of Module : {} failed.".format(
                    table_name, input_module
                )
            )
        
def checkTableJobStatus_CM(**kwargs):
    logger = kwargs["logging"]
    logger.info("In func : checkTableJobStatus_CM")
    table_name = kwargs["table_name"]
    input_module = kwargs["module"]
    branch = kwargs["branch_code"]
    jobId = kwargs["jobId"]
    data_pipeline = kwargs["data_pipeline"]
    el_run_date = kwargs["dag_run"].conf.get("elRunDate")
    entity_code = kwargs["dag_run"].conf.get("entityCode")

    jobStatus_URL = os.path.join(kwargs["data_pipeline"]["jobStatus_URL"])
    params_json = {}
    
    params_json.update({"jobId": jobId})
    logger.info("params_json : {}".format(params_json))

    for i in range(5):
        resultJobStatus = requests.post(jobStatus_URL, json=params_json)
        
        resultJobStatus = resultJobStatus.json()
        logger.info("Response resultJobStatus : {}".format(resultJobStatus))
        responseStatus = resultJobStatus["responseStatus"]
        jobStatusModel = resultJobStatus["jobStatusModel"]
        jobStatus = jobStatusModel["jobStatus"]
        tableCountMatch = jobStatusModel["tableCountMatch"]

        if jobStatus == "FINISHED" and tableCountMatch == True:
            tableJobStatus = "SUCCESS"
            break
        elif jobStatus == "FINISHED" and tableCountMatch == False:
            tableJobStatus = "FAIL"
            break
        else:
            time.sleep(60)
    if tableJobStatus != "SUCCESS":
        logger.info(
            "Response : {}".format(
                resultJobStatus["responseStatus"]
            )
        )
        logger.error(
            "Job for Table : {} of Module : {} failed.".format(table_name, input_module)
        )
        raise Exception(
            "Job for Table : {} of Module : {} failed.".format(table_name, input_module)
        )
    return tableJobStatus


def validateInput_CM(**kwargs):
    logger = kwargs["logging"]
    logger.info("In func : validateInput_CM")
    no_of_errors = 0
    current_dag_run_el_run_date = kwargs["dag_run"].conf.get("elRunDate")
    current_dag_run_entity_code = kwargs["dag_run"].conf.get("entityCode")
    logger.info(
        "current el_run_date : {} current_dag_run_entity_code : {}".format(
            current_dag_run_el_run_date, current_dag_run_entity_code
        )
    )
    if current_dag_run_entity_code is None or not current_dag_run_entity_code:
        no_of_errors = 1
        logger.error("Entity code (ENTITY_CODE) should be provided")

    if current_dag_run_el_run_date is None or not current_dag_run_el_run_date:
        no_of_errors = 1
        logger.error("EL Run date (EL_RUN_DATE) should be provided")
    else:
        try:
            d = datetime.strptime(current_dag_run_el_run_date, "%d-%b-%Y")
        except ValueError as ve:
            no_of_errors = 1
            logger.error(
                "EL Run date (EL_RUN_DATE) is of invalid format. "
                "It should be in the form of '%d-%b-%Y'. E.g: '10-SEP-2020'"
            )

    if no_of_errors > 0:
        logger.error("Inputs invalid. Please check")
        raise Exception("Inputs invalid. Please check")
    return
