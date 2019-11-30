import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from functools import partial

from .dag_utils import task_fail_slack_alert_callback
from data_pipeline.config import conf
from data_pipeline.utils.datetime_utils import generate_timestamp
from data_pipeline.db.mysql import connect_mysql, mysql_insert_transactions, mysql_insert_contacts
from data_pipeline.db.postgresql import (
    read_latest_postgres_data,
    fetch_latest_completed_sync_ts,
    upsert_into_data_sync_table,
    connect_postgresql,
)
from data_pipeline.utils.pipeline_constants import SyncStatusEnum
import data_pipeline.db.mysql as mysql

# Logger
logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def execute_sync_process_func(**kwargs):
    mysql_connection = connect_mysql()
    pg_connection = connect_postgresql()
    conf['MYSQL_STATSDB_CONNECTION'] = mysql_connection
    try:
        start_ts = generate_timestamp()
        logger.info(f'Starting the Data sync process - {start_ts}')

        mysql.ID = 0
        for key, table_name in conf.get('TABLES_MAPPING').items():

            # Fetch latest_processing_ts and last inserted_object_id from data_sync_stats table
            latest_processing_ts, to_id = fetch_latest_completed_sync_ts(pg_connection, key)

            logger.info(
                f'Latest updated timestamp from data_sync_stats table - {latest_processing_ts}'
            )
            logger.info(f'Last inserted Row Id - {to_id}')

            # Fetch rows from Postgresql based on the processing_ts and last_row_id
            query_result = read_latest_postgres_data(
                pg_connection, table_name, latest_processing_ts, to_id
            )
            logger.info(f'Number of {key} records to upsert - {len(query_result)}')

            partial_upsert_sync_stats = partial(
                upsert_into_data_sync_table,
                connection=pg_connection,
                dag_run_id=kwargs.get('dag_run').run_id,
                entity_type=key,
            )
            if len(query_result) > 0:

                first_row_id = query_result[0]['id']
                last_row_id = query_result[-1]['id']

                # Insert row into data_sync_stats table indicating sync started
                partial_upsert_sync_stats(
                    processing_ts=latest_processing_ts,
                    records_count=len(query_result),
                    from_id=first_row_id,
                    to_id=last_row_id,
                    status=SyncStatusEnum.STARTED.value,
                )

                # Insert records into MySQL
                if key == 'contact':
                    mysql_insert_contacts(
                        mysql_connection, pg_connection, query_result, kwargs.get('dag_run').run_id
                    )
                elif key == 'transaction':
                    mysql_insert_transactions(
                        mysql_connection, pg_connection, query_result, kwargs.get('dag_run').run_id
                    )

                # Update final status in data_sync_stats table
                partial_upsert_sync_stats(
                    processing_ts=query_result[-1]['processing_ts'],
                    records_count=len(query_result),
                    from_id=first_row_id,
                    to_id=last_row_id,
                    status=SyncStatusEnum.COMPLETED.value,
                )

                mysql.ID = 0
            else:
                # Update final status in data_sync_stats table
                partial_upsert_sync_stats(
                    processing_ts=latest_processing_ts,
                    records_count=len(query_result),
                    from_id=-1,
                    to_id=-1,
                    status=SyncStatusEnum.NO_DATA.value,
                )
            logger.info(
                'Completed the Data sync process - {} | Time taken - {}'.format(
                    generate_timestamp(),
                    (
                        round(generate_timestamp().timestamp() * 1000)
                        - round(start_ts.timestamp() * 1000)
                    ),
                )
            )
    except Exception as e:
        logger.exception('Exception in Postgresql MySQL Sync process')
    finally:
        if mysql_connection.is_connected():
            mysql_connection.close()
        if not pg_connection.closed:
            pg_connection.close()


default_args = {
    'owner': 'Airflow',
    'start_date': days_ago(30),
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert_callback,
}

postgresql_mysql_sync_dag = DAG(
    'POSTGRESQL_MYSQL_SYNC_DAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    max_active_runs=1,
)

postgresql_mysql_sync_task = PythonOperator(
    dag=postgresql_mysql_sync_dag,
    task_id='postgresql_mysql_sync_task',
    provide_context=True,
    python_callable=execute_sync_process_func,
)
