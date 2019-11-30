import os
import json
import errno
import logging
import time

from bson import json_util
from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from data_pipeline.utils.datetime_utils import get_current_ts_string
from data_pipeline.pipeline import reader, transformation, enrichment, persist
from data_pipeline import config
from data_pipeline.config import conf, get_s3_resource
from airflow.hooks.base_hook import BaseHook
from data_pipeline.db.update_stats_utils import create_connection
from data_pipeline.utils.pipeline_constants import EntityEnum

# Logger
logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


SLACK_CONN_ID = Variable.get('SLACK_CONN_ID', default_var=None)


def task_fail_slack_alert_callback(context):
    """
    Function to send a slack alert notification in case of a task failure
    Args:
        context:

    Returns: Instance of Slack Webhook Operator

    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    ti = context.get('task_instance')
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Dag Run Id*: {dag_run_id} 
            *Execution Time*: {exec_date}
            *Input File locations*: {input_files}  
            *Log Url*: {log_url} 
            """.format(
        task=ti.task_id,
        dag=ti.dag_id,
        dag_run_id=context.get('dag_run').run_id,
        ti=ti,
        exec_date=context.get('execution_date'),
        input_files=context.get('dag_run').conf.get('metadata').get('file_path'),
        log_url=ti.log_url,
    )

    temp_file_path_contacts = ti.xcom_pull('read_input_file_task', key='temp_file_path_contacts')
    temp_file_path_transactions = ti.xcom_pull(
        'read_input_file_task', key='temp_file_path_transactions'
    )

    # Delete Temp JSON Files
    delete_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_contacts
    ) if temp_file_path_contacts else None
    delete_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_transactions
    ) if temp_file_path_transactions else None

    failed_alert = SlackWebhookOperator(
        task_id='slack_error_notif',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='Airflow',
    )
    # TODO Update the processing status as failed
    return failed_alert.execute(context=context)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert_callback,
}

DATA_PIPELINE_DAG_NAME = 'PARALLELIZED_DATA_PIPELINE_DAG'
data_pipeline_dag = DAG(DATA_PIPELINE_DAG_NAME, default_args=default_args, schedule_interval=None)

# Create connection mysql statsdb
conf['MYSQL_STATSDB_CONNECTION'] = create_connection()


def read_json_file_s3(bucket_name, file_path):
    before_read = time.time()
    content_object = get_s3_resource().Object(bucket_name, file_path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content, object_hook=json_util.object_hook)
    after_read = time.time()
    logger.info(f'Temp File Read Time - {round(after_read - before_read)}')
    return json_content, round(after_read - before_read)


def put_json_file_s3(bucket_name, file_path, data):
    before_write = time.time()
    s3object = get_s3_resource().Object(bucket_name, file_path)
    s3object.put(Body=(bytes(json.dumps(data, default=json_util.default).encode('UTF-8'))))
    after_write = time.time()
    logger.info(f'Temp File Write Time - {round(after_write - before_write)}')
    return round(after_write - before_write)


def read_json_file_local(bucket_name, file_path):
    before_read = time.time()

    with open(file_path, "r") as f:
        file_content = f.read()
    json_content = json.loads(file_content, object_hook=json_util.object_hook)
    # content_object = s3.Object(bucket_name, file_path)
    # file_content = content_object.get()['Body'].read().decode('utf-8')
    # json_content = json.loads(file_content, object_hook=json_util.object_hook)
    after_read = time.time()
    logger.info(f'Temp File Read Time - {round(after_read - before_read)}')
    return json_content, round(after_read - before_read)


def put_json_file_local(bucket_name, file_path, data):
    before_write = time.time()
    # s3object = s3.Object(bucket_name, file_path)
    # s3object.put(
    #     Body=(bytes(json.dumps(data, default=json_util.default).encode('UTF-8')))
    # )
    if not os.path.exists(os.path.dirname(file_path)):
        try:
            os.makedirs(os.path.dirname(file_path))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
    with open(file_path, "w") as f:
        f.write(json.dumps(data, default=json_util.default))

    after_write = time.time()
    logger.info(f'Temp File Write Time - {round(after_write - before_write)}')
    return round(after_write - before_write)


def delete_file_s3(bucket_name, file_path):
    s3object = get_s3_resource().Object(bucket_name, file_path)
    s3object.delete()


def delete_file_local(bucket_name, file_path):
    os.remove(file_path)


def read_input_file_func(**kwargs):
    """
    Function to read input file from S3
    Args:
        **kwargs:

    Returns: None

    """
    metadata = kwargs.get('dag_run').conf.get('metadata')

    file_path_components = metadata.get('file_path').get('path').split('/')
    file_path_components[0] = f'{file_path_components[0]}_temp'
    temp_file_path = '/'.join(file_path_components)
    temp_file_path_contacts = f'{temp_file_path}_contacts.json'
    temp_file_path_transactions = f'{temp_file_path}_transactions.json'
    config.stats_entry_created_at = get_current_ts_string()
    config.dag_run_id = kwargs.get('dag_run').run_id
    data = reader.ReaderStep(metadata, []).process()

    contacts_data = {EntityEnum.CONTACT.value: data.get(EntityEnum.CONTACT.value)}
    transactions_data = {EntityEnum.TRANSACTION.value: data.get(EntityEnum.TRANSACTION.value)}
    kwargs.get('task_instance').xcom_push(
        key='temp_file_path_contacts', value=temp_file_path_contacts
    )
    kwargs.get('task_instance').xcom_push(
        key='temp_file_path_transactions', value=temp_file_path_transactions
    )
    kwargs.get('task_instance').xcom_push(key='metadata', value=metadata)
    kwargs.get('task_instance').xcom_push(key='log_stats_id', value=config.ID)

    contacts_time = put_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_contacts, contacts_data
    )
    transactions_time = put_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_transactions, transactions_data
    )

    return contacts_time, transactions_time


def transform_contact_data_func(**kwargs):
    """
    Function to transform the contacts data
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_contacts')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, contacts_read_time = read_json_file_local(conf.get('BUCKET_NAME', None), temp_file_path)

    data = transformation.CustomerTransformationStep(metadata, data).process()

    contacts_write_time = put_json_file_local(conf.get('BUCKET_NAME', None), temp_file_path, data)

    return contacts_read_time, contacts_write_time


def transform_transaction_data_func(**kwargs):
    """
    Function to transform the transactions data
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_transactions')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, transactions_read_time = read_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path
    )

    data = transformation.TransactionTransformationStep(metadata, data).process()

    transactions_write_time = put_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path, data
    )

    return transactions_read_time, transactions_write_time


def enrich_transformed_contacts_data_func(**kwargs):
    """
    Function to Enrich the transformed contacts data
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_contacts')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, contacts_read_time = read_json_file_local(conf.get('BUCKET_NAME', None), temp_file_path)

    data = enrichment.EnrichmentStep(metadata, data).process()

    contacts_write_time = put_json_file_local(conf.get('BUCKET_NAME', None), temp_file_path, data)

    return contacts_read_time, contacts_write_time


def enrich_transformed_transactions_data_func(**kwargs):
    """
    Function to Enrich the transformed transactions data
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_transactions')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, transactions_read_time = read_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path
    )

    data = enrichment.EnrichmentStep(metadata, data).process()

    transactions_write_time = put_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path, data
    )

    return transactions_read_time, transactions_write_time


def persist_contacts_output_data_func(**kwargs):
    """
    Function to persist the contacts data onto PostgreSQL
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_contacts')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, contacts_read_time = read_json_file_local(conf.get('BUCKET_NAME', None), temp_file_path)

    persist.PersistStep(metadata, data).process()

    return contacts_read_time


def persist_transactions_output_data_func(**kwargs):
    """
    Function to persist the transactions data onto PostgreSQL
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    config.ID = int(ti.xcom_pull('read_input_file_task', key='log_stats_id'))
    temp_file_path = ti.xcom_pull('read_input_file_task', key='temp_file_path_transactions')
    metadata = ti.xcom_pull('read_input_file_task', key='metadata')

    data, transactions_read_time = read_json_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path
    )

    persist.PersistStep(metadata, data).process()

    return transactions_read_time


def data_pipeline_process_complete_func(**kwargs):
    """
    Function to perform the postprocessing activities like temp data cleanup, etc.
    Args:
        **kwargs:

    Returns: None

    """
    ti = kwargs.get('task_instance')
    temp_file_path_contacts = ti.xcom_pull('read_input_file_task', key='temp_file_path_contacts')
    temp_file_path_transactions = ti.xcom_pull(
        'read_input_file_task', key='temp_file_path_transactions'
    )

    # Delete Temp JSON Files
    delete_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_contacts
    ) if temp_file_path_contacts else None
    delete_file_local(
        conf.get('BUCKET_NAME', None), temp_file_path_transactions
    ) if temp_file_path_transactions else None
    # TODO delete xcom
    return None


read_input_file_task = PythonOperator(
    task_id='read_input_file_task',
    provide_context=True,
    python_callable=read_input_file_func,
    dag=data_pipeline_dag,
)

contacts_data_transformation_task = PythonOperator(
    task_id='contacts_data_transformation_task',
    provide_context=True,
    python_callable=transform_contact_data_func,
    dag=data_pipeline_dag,
)

transactions_data_transformation_task = PythonOperator(
    task_id='transactions_data_transformation_task',
    provide_context=True,
    python_callable=transform_transaction_data_func,
    dag=data_pipeline_dag,
)

enrich_transformed_contacts_data_task = PythonOperator(
    task_id='enrich_transformed_contacts_data_task',
    provide_context=True,
    python_callable=enrich_transformed_contacts_data_func,
    dag=data_pipeline_dag,
)

enrich_transformed_transactions_data_task = PythonOperator(
    task_id='enrich_transformed_transactions_data_task',
    provide_context=True,
    python_callable=enrich_transformed_transactions_data_func,
    dag=data_pipeline_dag,
)

persist_contacts_output_data_task = PythonOperator(
    task_id='persist_contacts_output_data_task',
    provide_context=True,
    python_callable=persist_contacts_output_data_func,
    dag=data_pipeline_dag,
)

persist_transactions_output_data_task = PythonOperator(
    task_id='persist_transactions_output_data_task',
    provide_context=True,
    python_callable=persist_transactions_output_data_func,
    dag=data_pipeline_dag,
)

data_pipeline_process_complete_task = PythonOperator(
    dag=data_pipeline_dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id='data_pipeline_process_complete_task',
    provide_context=True,
    python_callable=data_pipeline_process_complete_func,
)

read_input_file_task >> contacts_data_transformation_task >> enrich_transformed_contacts_data_task >> persist_contacts_output_data_task >> data_pipeline_process_complete_task
read_input_file_task >> transactions_data_transformation_task >> enrich_transformed_transactions_data_task >> persist_transactions_output_data_task >> data_pipeline_process_complete_task
