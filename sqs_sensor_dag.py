import json
import logging
from io import BytesIO
from zipfile import ZipFile

from airflow import DAG
from .dag_utils import task_fail_slack_alert_callback
from airflow.utils.decorators import apply_defaults
from airflow.settings import Session, DAGS_FOLDER
from airflow.models.dag import DagBag
from airflow.utils.state import State
from airflow.operators.dagrun_operator import DagRunOrder, TriggerDagRunOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from data_pipeline.config import conf, get_s3_resource, read_mapping_config

# Logger
logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATA_PIPELINE_DAG_NAME = 'PARALLELIZED_DATA_PIPELINE_DAG'


class TriggerMultiDagRunOperator(TriggerDagRunOperator):
    @apply_defaults
    def __init__(self, op_args=None, op_kwargs=None, provide_context=True, *args, **kwargs):
        super(TriggerMultiDagRunOperator, self).__init__(*args, **kwargs)
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context

    def execute(self, context):
        session = Session()
        created = False
        for dro in self.python_callable(context, *self.op_args, **self.op_kwargs):
            if not dro or not isinstance(dro, DagRunOrder):
                break

            if dro.run_id is None:
                dro.run_id = 'trig__' + datetime.utcnow().isoformat()

            dbag = DagBag(DAGS_FOLDER)
            trigger_dag = dbag.get_dag(self.trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id, state=State.RUNNING, conf=dro.payload, external_trigger=True
            )
            created = True
            self.log.info("Creating DagRun %s", dr)

        if created is True:
            session.commit()
        else:
            self.log.info("No DagRun created")
        session.close()


def process_sqs_events(event: dict):
    """
    Function to process the SQS events received by the Lambda handler

    Args:
        event: SQS event object

    Returns:
        file_paths (list)

    """
    file_paths = []
    for message in event.get('Messages'):
        s3_file_details = json.loads(message.get('Body'))
        file_paths.append(
            {
                'bucket': s3_file_details.get('s3_bucket'),
                'path': s3_file_details.get('s3_file_path'),
                'size': get_s3_resource()
                .Object(s3_file_details.get('s3_bucket'), s3_file_details.get('s3_file_path'))
                .content_length,
            }
        )
    return file_paths


def trigger_data_pipeline_func(context):
    sqs_message = context.get('task_instance').xcom_pull('sqs_message_sensor_task', key='messages')
    logger.info(sqs_message)
    file_paths = process_sqs_events(sqs_message)
    if file_paths:
        for file in file_paths:
            # Read metadata file
            zip_file_obj = get_s3_resource().Object(
                bucket_name=file.get('bucket'), key=file.get('path')
            )
            file_buffer = BytesIO(zip_file_obj.get()["Body"].read())
            archive = ZipFile(file_buffer)
            metadata_file_content = archive.read('metadata.json')
            metadata = json.loads(metadata_file_content.decode('UTF-8'))

            # Get mapping schema based on values picked up from metadata file
            mapping_schema = read_mapping_config(
                metadata.get('workflow_system_name'), metadata.get('company_id')
            )

            yield DagRunOrder(
                payload={
                    'metadata': {'file_path': file, 'schema': mapping_schema, 'metadata': metadata}
                }
            )


default_args = {
    'owner': 'Airflow',
    'start_date': days_ago(30),
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert_callback,
}

sqs_sensor_dag = DAG(
    'SQS_SENSOR_DAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    max_active_runs=1,
)

sqs_message_sensor_task = SQSSensor(
    dag=sqs_sensor_dag,
    task_id='sqs_message_sensor_task',
    aws_conn_id=conf.get('AWS_CONN_ID', None),
    wait_time_seconds=10,
    sqs_queue=conf.get('SQS_QUEUE_URL', None),
)

trigger_data_pipeline_task = TriggerMultiDagRunOperator(
    task_id='trigger_data_pipeline_task',
    dag=sqs_sensor_dag,
    python_callable=trigger_data_pipeline_func,
    trigger_dag_id=DATA_PIPELINE_DAG_NAME,
)

sqs_message_sensor_task >> trigger_data_pipeline_task
