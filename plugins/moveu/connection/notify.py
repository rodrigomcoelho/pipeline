from airflow.models.variable import Variable
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.email import send_email_smtp

# This should match the connection ID created in the Medium article
SLACK_CONN_ID = Variable.get("SLACK_CONN_ID")  # "airflow_alerts"
CHANNEL = Variable.get("CHANNEL")  #'airflow-alerts-de'

# SLACK_CONN_ID = ''
TOKEN = "xoxb-4865145097-2471264219895-EExBkCz4KsbjHu7k9bLvVZVq"
# CHANNEL = 'airflow-alerts-de'


def send_email(to, subject, html_content):
    send_email_smtp(to, subject, html_content)


def task_success_slack_alert(context):
    slack_msg = """
            :ultrafastparrot: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    slack_hook = SlackHook(token=TOKEN)
    slack_hook.client.chat_postMessage(channel=CHANNEL, text=slack_msg)


def task_fail_slack_alert(context):

    slack_msg = """
            :aaaaaaaaaaaaaaa: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    slack_hook = SlackHook(token=TOKEN)
    slack_hook.client.chat_postMessage(channel=CHANNEL, text=slack_msg)


def task_generic_slack_alert(slack_msg, **kwargs):

    slack_msg = slack_msg

    # slack_hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
    slack_hook = SlackHook(token=TOKEN)
    slack_hook.client.chat_postMessage(channel=CHANNEL, text=slack_msg)


def slack_alert(**kwargs):

    vslack_msg = kwargs["msg"]
    vtoken = kwargs["token"]
    vchannel = kwargs["channel"]

    # slack_hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
    slack_hook = SlackHook(token=vtoken)
    slack_hook.client.chat_postMessage(channel=vchannel, text=vslack_msg)
