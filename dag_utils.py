from airflow.hooks.base_hook import BaseHook
from airflow.models.variable import Variable
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = Variable.get('SLACK_CONN_ID', default_var=None)


def task_fail_slack_alert_callback(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    ti = context.get('task_instance')
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Dag Run Id*: {dag_run_id} 
            *Execution Time*: {exec_date}
            *Log Url*: {log_url} 
            """.format(
        task=ti.task_id,
        dag=ti.dag_id,
        dag_run_id=context.get('dag_run').run_id,
        ti=ti,
        exec_date=context.get('execution_date'),
        log_url=ti.log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id='slack_error_notif',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='Airflow',
    )
    # TODO Update the processing status as failed
    return failed_alert.execute(context=context)
