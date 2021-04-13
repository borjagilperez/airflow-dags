# -*- coding: utf-8 -*-

import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email

def __outer_email_success_callback(context, to, cc=None, bcc=None):

    subject = "[Airflow] DAG {0} - Task {1}: Success".format(
        context['task_instance_key_str'].split('__')[0],
        context['task_instance_key_str'].split('__')[1])

    html_content = f"""
    DAG: {context['task_instance_key_str'].split('__')[0]}<br>
    Task: {context['task_instance_key_str'].split('__')[1]}<br>
    Succeeded on: {context['ts']}<br>
    <br>
    Context = {context}
    """

    send_email(to=to, cc=cc, bcc=bcc, subject=subject, html_content=html_content)

dag_config_email = Variable.get("send_email", deserialize_json=True)
email_to = dag_config_email['EMAIL_TO']
email_cc = dag_config_email['EMAIL_CC']
email_bcc = dag_config_email['EMAIL_BCC']

def __email_success_callback(context):
    
    __outer_email_success_callback(context, to=email_to, cc=email_cc, bcc=email_bcc)

local_tz = pendulum.timezone('Europe/Madrid')
start_date = datetime.strptime("2021-03-28 13:30:00", '%Y-%m-%d %H:%M:%S').replace(tzinfo=local_tz)
default_args = {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': start_date,
    'email': email_to,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    "spark_awssecrets_example",
    schedule_interval='@once',
    catchup=False,
    default_args=default_args) as dag:


    dag_config_spark = Variable.get("spark_k8s", deserialize_json=True)
    t1 = BashOperator(
        task_id="spark_awssecrets",
        bash_command=f'''
            TMP_DIR='/tmp/spark/local/spark_awssecrets_example/spark_awssecrets' && mkdir -p $TMP_DIR && \\
            export SPARK_HOME=$HOME/spark3 && export PATH=$SPARK_HOME/bin:$PATH && \\
            LAUNCHER=/opt/spark/work-dir/examples/awssecrets.py && \\
            spark-submit \\
                --name awssecrets-example \\
                --master k8s://{dag_config_spark['K8S_MASTER']} \\
                --deploy-mode cluster \\
                --conf spark.kubernetes.namespace=spark \\
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
                --conf spark.kubernetes.container.image={dag_config_spark['SPARK_DOCKER_IMG']} \\
                --conf spark.kubernetes.container.image.pullPolicy=Always \\
                --conf spark.kubernetes.pyspark.pythonVersion=3 \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_S3_ENDPOINT_URL=aws-secret:aws-s3-endpoint-url \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_S3_SIGNATURE_VERSION=aws-secret:aws-s3-signature-version \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=aws-secret:aws-access-key-id \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=aws-secret:aws-secret-access-key \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_S3_REGION_NAME=aws-secret:aws-s3-region-name \\
                --conf spark.kubernetes.driver.secretKeyRef.AWS_STORAGE_BUCKET_NAME=aws-secret:aws-storage-bucket-name \\
                --conf spark.driver.cores={dag_config_spark['size_m']['DRIVER_CORES']} \\
                --conf spark.driver.memory={dag_config_spark['size_m']['DRIVER_MEMORY']} \\
                --conf spark.executor.instances={dag_config_spark['size_m']['EXECUTOR_INSTANCES']} \\
                --conf spark.executor.cores={dag_config_spark['size_m']['EXECUTOR_CORES']} \\
                --conf spark.executor.memory={dag_config_spark['size_m']['EXECUTOR_MEMORY']} \\
                local://$LAUNCHER \\
                2>&1 | tee $TMP_DIR/spark-submit-client.log && \\
            python3 $HOME/Git/bdds-platform/spark-kubernetes/src/main/python/scripts/spark_check_logs.py check $TMP_DIR/spark-submit-client.log
        '''
    )

    t_email = DummyOperator(
        task_id="send_email",
        on_success_callback=__email_success_callback
    )

    t1 >> t_email
