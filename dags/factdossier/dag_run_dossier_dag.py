from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with DAG(
    dag_id="run_dossier_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("Début du déclenchement des DAGs..."),
        dag=dag
    )

    trigger_entreprise = TriggerDagRunOperator(
        task_id="trigger_dag_dim_entreprise",
        trigger_dag_id="dag_dim_entreprise",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_recruteur = TriggerDagRunOperator(
        task_id="trigger_dag_dim_recruteur",
        trigger_dag_id="Dag_dim_recruteur",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_universites = TriggerDagRunOperator(
        task_id="trigger_dag_dim_universites",
        trigger_dag_id="dag_dim_universites",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_formations = TriggerDagRunOperator(
        task_id="trigger_dag_dim_formations",
        trigger_dag_id="dag_dim_formations",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_offres_emplois = TriggerDagRunOperator(
        task_id="trigger_dag_dim_offre_emplois",
        trigger_dag_id="dag_dim_offre_emplois",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_offres_etude = TriggerDagRunOperator(
        task_id="trigger_dag_dim_offre_etude",
        trigger_dag_id="dag_dim_offre_etude",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_villes = TriggerDagRunOperator(
        task_id="trigger_dag_dim_villes",
        trigger_dag_id="dag_dim_villes",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_services = TriggerDagRunOperator(
        task_id="trigger_dag_dim_service",
        trigger_dag_id="dag_dim_service",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_fact_dossier = TriggerDagRunOperator(
        task_id="trigger_dag_fact_profile_dossier",
        trigger_dag_id="dag_fact_profile_dossier",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Fin du déclenchement des DAGs."),
        dag=dag
    )

    trigger_metiers = TriggerDagRunOperator(
        task_id="trigger_dag_dim_metier",
        trigger_dag_id="dag_dim_metier",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_secteurs = TriggerDagRunOperator(
        task_id="trigger_dag_dim_secteur",
        trigger_dag_id="dag_dim_secteur",
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    # Définition des groupes de tâches
    first_layer = [trigger_universites, trigger_entreprise, trigger_metiers, trigger_secteurs]
    second_layer = [
        trigger_recruteur,
        trigger_formations,
        trigger_offres_emplois,
        trigger_offres_etude,
        trigger_villes,
        trigger_services
    ]

    # Configuration des dépendances avec la même logique
    start_task >> first_layer
    for first_task in first_layer:
        first_task >> second_layer
    for second_task in second_layer:
        second_task >> trigger_fact_dossier
    trigger_fact_dossier >> end_task