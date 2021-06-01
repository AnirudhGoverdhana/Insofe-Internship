import os
import psycopg2
import pickle
import pandas as pd
from datetime import datetime, timedelta


def getTasksFailedCount_DB(**kwargs):
    postgres_connection, record, cursor = None, None, None
    logger = kwargs["logging"]
    logger.info("In func : getTasksFailedCount_DB")
    return_value = 0
    try:
        postgres_connection = psycopg2.connect(
                    host=kwargs["postgres"]["hostname"],
                    database=kwargs["postgres"]["airflow_db_name"],
                    user=kwargs["postgres"]["airflow_db_user"],
                    password=kwargs["postgres"]["airflow_db_password"]
                )
        cursor = postgres_connection.cursor()
        # get run_id ,execution_date of current run
        current_exe_date_time = kwargs["dag_run"].execution_date
        current_exe_date = current_exe_date_time.strftime("%Y-%m-%d")
        current_run_id = kwargs["dag_run"].run_id
        dag_id = kwargs["dag_run"].dag_id
        # check any task failed for this dag and execution_date
        sql_select_query = """select count(*) as cnt from task_instance where dag_id=%s and execution_date=%s and state=%s"""
        cursor.execute(sql_select_query, (dag_id, current_exe_date_time, 'failed'))
        record = cursor.fetchone()
        if record is not None:
            return_value = record[0]
            logger.info("run_id : {} No of tasks failed : {}".format(current_run_id, return_value))
    except (Exception, psycopg2.Error) as error:
        logger.error("Failed to get record from PostgresSQL table: {}".format(error))
    finally:
        if postgres_connection is None:
            logger.error("Postgres connection not established")
            raise Exception("Postgres connection not established")
        else:
            cursor.close()
            postgres_connection.close()
            logger.info("PostgresSQL connection is closed")
    return return_value





def updateDAG_DB(**kwargs):
    logger = kwargs["logging"]
    logger.info("In func : updateDAG_DB")
    branch_code = kwargs["branch_code"]
    el_run_date = kwargs["dag_run"].conf.get("elRunDate")
    entity_code = kwargs["dag_run"].conf.get("entityCode")
    current_run_id = kwargs["dag_run"].run_id
    current_exe_date_time = kwargs["dag_run"].execution_date
    dag_id = kwargs["dag_run"].dag_id
    logger.info("el_run_date : {} entity_code : {} current_exe_date_time : {}".format(el_run_date, entity_code, current_exe_date_time))
    task_failed_count = getTasksFailedCount_DB(**kwargs)
    if task_failed_count > 0:
        err_msg = "One or more tasks failed for dag : {} run_id : {} execution_date : {}".format(
            dag_id, current_run_id, current_exe_date_time
        )
        logger.error(err_msg)
        raise Exception(err_msg)
    return "SUCCESS"

 


def checkPreviousRunForDAG_DB(**kwargs):
    postgres_connection, record, cursor = None, None, None
    logger = kwargs["logging"]
    logger.info("In func : checkPreviousRunForDAG_DB")
    return_value = None
    try:
        postgres_connection = psycopg2.connect(
                    host=kwargs["postgres"]["hostname"],
                    database=kwargs["postgres"]["airflow_db_name"],
                    user=kwargs["postgres"]["airflow_db_user"],
                    password=kwargs["postgres"]["airflow_db_password"]
                )
        cursor = postgres_connection.cursor()
        current_exe_date_time = kwargs["dag_run"].execution_date
        current_exe_date = current_exe_date_time.strftime("%Y-%m-%d")
        sql_select_query = """select to_char(execution_date, %s) , state from dag_run where dag_id = %s
                                    and execution_date < %s order by execution_date desc limit 1"""
        cursor.execute(sql_select_query, ('yyyy-mm-dd', kwargs["dag_run"].dag_id, current_exe_date_time))
        record = cursor.fetchone()
        current_dag_run_el_run_date = kwargs["dag_run"].conf.get("elRunDate")
        current_dag_run_entity_code = kwargs["dag_run"].conf.get("entityCode")
        prev_dag_run, prev_dag_run_el_run_date, prev_dag_run_entity_code = None, None, None
        prev_dag_run = kwargs["dag_run"].get_previous_dagrun()
        if prev_dag_run:
            prev_dag_run_el_run_date = prev_dag_run.conf.get("elRunDate")
            prev_dag_run_entity_code = prev_dag_run.conf.get("entityCode")
        logger.info("current el_run_date : {} previous el_run_date : {}".format(current_dag_run_el_run_date, prev_dag_run_el_run_date))
        if record is None:
            return_value = "FAIL_OR_NOTRUN"
        else:
            prev_exe_date = record[0]  # previous execution date only - after to_char in the above query
            state = record[1]
            logger.info("current execution_date : {} previous execution_date : {}".format(current_exe_date, prev_exe_date))
            if current_dag_run_el_run_date == prev_dag_run_el_run_date and current_dag_run_entity_code == prev_dag_run_entity_code:
                if state == "success":
                    return_value = "SUCCESS"
                elif state == "running":
                    return_value = "PREV_RUN_NOT_COMPLETE"
                else:
                    return_value = "FAIL_OR_NOTRUN"
            else:
                return_value = "FAIL_OR_NOTRUN"
        logger.info("return_value : {}".format(return_value))
    except (Exception, psycopg2.Error) as error:
        logger.error("Failed to get record from PostgresSQL table: {}".format(error))
    finally:
        if postgres_connection is None:
            logger.error("Postgres connection not established")
            raise Exception("Postgres connection not established")
        else:
            cursor.close()
            postgres_connection.close()
            logger.info("PostgresSQL connection is closed")

    if return_value == "PREV_RUN_NOT_COMPLETE":
        raise Exception("Previous run not complete")
    task_instance = kwargs["task_instance"]
    task_instance.xcom_push("previous_dag_run_status", value=return_value)
    return return_value


def checkPreviousRunForTable_DB(**kwargs):
    postgres_connection, record, cursor = None, None, None
    logger = kwargs["logging"]
    logger.info("In func : checkPreviousRunForTable_DB")
    return_value = None
    try:
        postgres_connection = psycopg2.connect(
            host=kwargs["postgres"]["hostname"],
            database=kwargs["postgres"]["airflow_db_name"],
            user=kwargs["postgres"]["airflow_db_user"],
            password=kwargs["postgres"]["airflow_db_password"]
        )
        cursor = postgres_connection.cursor()
        current_exe_date_time = kwargs["dag_run"].execution_date
        current_exe_date = current_exe_date_time.strftime("%Y-%m-%d")
        sql_select_query = """select to_char(execution_date, %s) , state from task_instance where dag_id = %s
                                    and execution_date < %s and task_id = %s order by execution_date desc limit 1"""
        cursor.execute(sql_select_query, ('yyyy-mm-dd', kwargs["dag_run"].dag_id, current_exe_date_time, kwargs["table_name"]))
        record = cursor.fetchone()
        current_dag_run_el_run_date = kwargs["dag_run"].conf.get("elRunDate")
        current_dag_run_entity_code = kwargs["dag_run"].conf.get("entityCode")
        prev_dag_run, prev_dag_run_el_run_date, prev_dag_run_entity_code = None, None, None
        prev_dag_run = kwargs["dag_run"].get_previous_dagrun()
        if prev_dag_run and prev_dag_run.conf is not None:
            prev_dag_run_el_run_date = prev_dag_run.conf.get("elRunDate")
            prev_dag_run_entity_code = prev_dag_run.conf.get("entityCode")
        logger.info("current el_run_date : {} previous el_run_date : {}".format(current_dag_run_el_run_date, prev_dag_run_el_run_date))
        if record is None:
            return_value = "FAIL"
        else:
            prev_exe_date = record[0]  # previous execution date only - after to_char in the above query
            state = record[1]
            logger.info("current execution_date : {} previous execution_date : {}".format(current_exe_date, prev_exe_date))
            if current_dag_run_el_run_date == prev_dag_run_el_run_date and current_dag_run_entity_code == prev_dag_run_entity_code:
                if state == "success":
                    return_value = "SUCCESS"
                else:
                    return_value = "FAIL"
            else:
                return_value = "FAIL"
        logger.info("return_value : {}".format(return_value))
    except (Exception, psycopg2.Error) as error:
        logger.error("Failed to get record from PostgresSQL table: {}".format(error))
    finally:
        if postgres_connection is None:
            logger.error("Postgres connection not established")
            raise Exception("Postgres connection not established")
        else:
            cursor.close()
            postgres_connection.close()
            logger.info("PostgresSQL connection is closed")
    task_instance = kwargs["task_instance"]
    if return_value == "FAIL":  # no previous run of table or previous run failed => so need to run kettle for the table
        task_instance.xcom_push(kwargs["table_name"] + "_RUN_FLAG", value=1)
    else:
        task_instance.xcom_push(kwargs["table_name"] + "_RUN_FLAG", value=0)
    return return_value


def getBankRunStatus_DB(**kwargs):
    postgres_connection, record, cursor = None, None, None
    logger = kwargs["logging"]
    logger.info("In func : getBankRunStatus_DB")
    return_value = None
    try:
        postgres_connection = psycopg2.connect(
                    host=kwargs["postgres"]["hostname"],
                    database=kwargs["postgres"]["airflow_db_name"],
                    user=kwargs["postgres"]["airflow_db_user"],
                    password=kwargs["postgres"]["airflow_db_password"]
                )
        cursor = postgres_connection.cursor()
        sql_select_query = """select conf,state from dag_run where dag_id='BANK' order by execution_date desc  limit 1"""
        cursor.execute(sql_select_query)
        record = cursor.fetchone()
        logger.info(record)
        if record is None:
            return_value = "FAIL_OR_NOTRUN"
        else:
            conf = pickle.loads(record[0])
            logger.info(conf)
            state = record[1]
            logger.info(state)
            current_dag_run_el_run_date = kwargs["dag_run"].conf.get("elRunDate")
            logger.error(current_dag_run_el_run_date)
            current_dag_run_entity_code = kwargs["dag_run"].conf.get("entityCode")
            logger.error(current_dag_run_entity_code)
            if current_dag_run_el_run_date == conf["elRunDate"] and current_dag_run_entity_code == conf["entityCode"]:
            
                if state == "success":
                    return_value = "SUCCESS"
                elif state == "running":
                    return_value = "PREV_RUN_NOT_COMPLETE"
                else:
                    return_value = "FAIL_OR_NOTRUN"
            else:
                return_value = "FAIL_OR_NOTRUN"
        logger.info("return_value : {}".format(return_value))
    except (Exception, psycopg2.Error) as error:
        logger.error("Failed to get record from PostgresSQL table: {}".format(error))
    finally:
        if postgres_connection is None:
            logger.error("Postgres connection not established")
            raise Exception("Postgres connection not established")
        else:
            cursor.close()
            postgres_connection.close()
            logger.info("PostgresSQL connection is closed")

    if return_value != "SUCCESS":
        raise Exception("Previous run not complete or Failed/Not run")
    return return_value
