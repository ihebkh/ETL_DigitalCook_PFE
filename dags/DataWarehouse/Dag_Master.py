from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with DAG(
    dag_id="run_ETL_dag",
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

    trigger_fact_recrutement = TriggerDagRunOperator(
        task_id="trigger_dag_fact_recrutement",
        trigger_dag_id="dag_fact_recrutement",
        conf={"manual_trigger": True},
        wait_for_completion=True
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

    trigger_pays = TriggerDagRunOperator(
    task_id='trigger_dag_dim_pays',
    trigger_dag_id='Dag_dim_pays',
    conf={"manual_trigger": True},
    wait_for_completion=True
    )

    trigger_secteur = TriggerDagRunOperator(
        task_id='trigger_dag_dim_secteur',
        trigger_dag_id='dag_dim_secteur',
        conf={"manual_trigger": True},
        wait_for_completion=True
    )

    trigger_client_profile = TriggerDagRunOperator(
    task_id='trigger_dag_fact_client_profile',
    trigger_dag_id='dag_client_profile',
    conf={"manual_trigger": True},
    wait_for_completion=True
)

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Fin du déclenchement des DAGs."),
        dag=dag
    )
    first_layer = [trigger_certifications, trigger_competences, trigger_experience,
                   trigger_formations, trigger_interet, trigger_langues, trigger_metier, trigger_niveau_etudes,
                   trigger_projets, trigger_recruteur, trigger_secteur, trigger_services]
    second_layer = [trigger_entreprise, trigger_villes, trigger_universites]
    third_layer = [trigger_offres_emplois, trigger_offres_etude]
    fourth_layer = [trigger_client_profile,trigger_fact_recrutement]

    start_task >> first_layer
    for first_task in first_layer:
        first_task >> second_layer
    for second_task in second_layer:
        second_task >> third_layer
    for third_task in third_layer:
        third_task >> fourth_layer
    for fourth_task in fourth_layer:
        fourth_task >> end_task
                    

                    
    

