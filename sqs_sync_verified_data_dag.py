import json
import logging

from airflow import DAG
from dag_utils import task_fail_slack_alert_callback
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from data_pipeline.config import conf
from data_pipeline.db.postgresql import connect_postgresql, update_verified_data

# Logger
logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def process_sqs_events(event: dict):
    """
    Function to process the SQS events received by the Lambda handler

    Args:
        event (dict): SQS event object

    Returns:
        verified_data (list):

    """
    verified_data = []
    for message in event.get('Messages'):
        data = json.loads(message.get('Body'))
        verified_data.append(data)
    return verified_data


def sync_verified_data_func(context):
    """
    Function to parse the SQS message
    Args:
        context:

    Returns: None

    """
    sqs_message = context.get('task_instance').xcom_pull('sqs_message_sensor_task', key='messages')
    logger.info(sqs_message)
    verified_data = process_sqs_events(sqs_message)
    if verified_data:
        pg_connection = connect_postgresql()
        for data in verified_data:
            res = update_verified_data(
                pg_connection,
                data.get('entity'),
                data.get('company_id'),
                data.get('external_id'),
                data.get('verified_data'),
            )
            if res:
                logger.info(
                    f'''Successfully updated the verified_data in `verified_{data.get('entity')}`
                    table for (company_id - {data.get('company_id')}, external_id - {data.get('external_id')})'''
                )
            else:
                logger.info(
                    f'''Failed to update the verified_data in `verified_{data.get('entity')}`
                    table for (company_id - {data.get('company_id')}, external_id - {data.get('external_id')})'''
                )


default_args = {
    'owner': 'Airflow',
    'start_date': days_ago(30),
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert_callback,
}

sqs_sensor_dag = DAG(
    'SQS_VERIFIED_DATA_SYNC_SENSOR_DAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    max_active_runs=1,
)

sqs_message_sensor_task = SQSSensor(
    dag=sqs_sensor_dag,
    task_id='sqs_message_sensor_task',
    aws_conn_id=conf.get('AWS_CONN_ID', None),
    wait_time_seconds=10,
    sqs_queue=conf.get('SQS_SYNC_VERIFIED_DATA_QUEUE_URL', None),
)

sync_verified_data_task = PythonOperator(
    task_id='sync_verified_data_task',
    dag=sqs_sensor_dag,
    python_callable=sync_verified_data_func,
    provide_context=True,
)

sqs_message_sensor_task >> sync_verified_data_task
