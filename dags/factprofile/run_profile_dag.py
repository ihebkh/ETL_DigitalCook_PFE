from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with DAG(
    dag_id="run_profile_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily", 
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("Starting region extraction process..."),
        dag=dag
    )

    trigger_certifications = TriggerDagRunOperator(
        task_id="trigger_dag_dim_certifications",
        trigger_dag_id="dag_dim_certifications",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_clients = TriggerDagRunOperator(
        task_id="trigger_dag_dim_clients",
        trigger_dag_id="dag_dim_Clients",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_competences = TriggerDagRunOperator(
        task_id="trigger_dag_dim_competences",
        trigger_dag_id="dag_dim_Competences",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_experience = TriggerDagRunOperator(
        task_id="trigger_dag_dim_experience",
        trigger_dag_id="dag_dim_experience",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_interet = TriggerDagRunOperator(
        task_id="trigger_dag_dim_interet",
        trigger_dag_id="dag_dim_interet",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_langues = TriggerDagRunOperator(
        task_id="trigger_dag_dim_languages",
        trigger_dag_id="dag_dim_languages",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_metier = TriggerDagRunOperator(
        task_id="trigger_dag_dim_metier",
        trigger_dag_id="dag_dim_metier",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_niveau_etudes = TriggerDagRunOperator(
        task_id="trigger_dag_dim_niveau_etudes",
        trigger_dag_id="dag_dim_niveau_etudes",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_projets = TriggerDagRunOperator(
        task_id='trigger_dag_dim_projet',
        trigger_dag_id='dag_dim_projet',
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_secteur = TriggerDagRunOperator(
        task_id='trigger_dag_dim_secteur',
        trigger_dag_id='dag_dim_secteur',
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_fact_profile = TriggerDagRunOperator(
        task_id='trigger_dag_fact_client_profile',
        trigger_dag_id='dag_client_profile',
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Region extraction process completed."),
        dag=dag
    )

start_task>>[trigger_certifications,trigger_clients,trigger_competences,trigger_experience,trigger_interet,
             trigger_langues,trigger_metier,trigger_niveau_etudes,trigger_projets,trigger_secteur]>>trigger_fact_profile>>end_task