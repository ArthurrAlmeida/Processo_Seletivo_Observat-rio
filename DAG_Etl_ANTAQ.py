from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import os

def check_data():
    file_path = "/data_lake/antaq/raw/antaq_data.parquet"
    if not os.path.exists(file_path):
        raise FileNotFoundError("Arquivo de dados não encontrado!")

def send_failure_email(context):
    email_task = EmailOperator(
        task_id="send_failure_email",
        to=["dgasilva@sfiec.org.br", "elgomes@sfiec.org.br", "rpadilha@sfiec.org.br"],
        subject="ETL ANTAQ Falhou",
        html_content=f"""
        <h3>Falha na DAG: {context['task_instance'].dag_id}</h3>
        <p>Erro: {context['exception']}</p>
        """
    )
    return email_task.execute(context=context)

def send_success_email():
    return EmailOperator(
        task_id="send_success_email",
        to=["dgasilva@sfiec.org.br", "elgomes@sfiec.org.br", "rpadilha@sfiec.org.br"],
        subject="ETL ANTAQ Concluído",
        html_content="<h3>Processo ETL concluído com sucesso!</h3>"
    ).execute({})

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_email
}

dag = DAG(
    "antaq_etl_pipeline",
    default_args=default_args,
    description="ETL para dados da ANTAQ",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
)

check_file = FileSensor(
    task_id="check_file_availability",
    filepath="/data_lake/antaq/raw/antaq_data.parquet",
    poke_interval=60,
    timeout=600,
    dag=dag
)

validate_data = PythonOperator(
    task_id="validate_data",
    python_callable=check_data,
    dag=dag
)

spark_etl = SparkSubmitOperator(
    task_id="spark_process_etl",
    application="/scripts/antaq_etl.py",
    conn_id="spark_default",
    dag=dag
)

load_to_sql = MsSqlOperator(
    task_id="load_to_sql",
    sql="EXEC dbo.sp_LoadAntaqData;",
    mssql_conn_id="mssql_default",
    autocommit=True,
    dag=dag
)

success_email = PythonOperator(
    task_id="send_success_email",
    python_callable=send_success_email,
    dag=dag
)

check_file >> validate_data >> spark_etl >> load_to_sql >> success_email
