from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_segmentation_update():
    # Récupérer la connexion via le hook Airflow
    hook = PostgresHook(postgres_conn_id='postgres')  # 'postgres' : nom de ta connexion Airflow
    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT 
            c.client_id,
            c.nom_client, 
            c.prenom_client, 
            s.nom_secteur AS secteur_client, 
            s.secteur_id AS secteur_id_client, 
            o.titre_offre_emploi AS offre_emploi, 
            o.niveau_experience,
            (fcp.annee_experience * 12 + fcp.mois_experience) AS duree_experience
        FROM 
            dim_client c
        JOIN 
            dim_client_profile fcp ON c.client_id = fcp.client_id
        JOIN 
            dim_secteur s ON fcp.secteur_id = s.secteur_id
        JOIN 
            dim_offre_emploi o ON fcp.secteur_id = o.secteur_id
        WHERE 
            (
                (o.niveau_experience = 'Débutant' AND (fcp.annee_experience * 12 + fcp.mois_experience) BETWEEN 0 AND 12)
                OR (o.niveau_experience = 'Junior' AND (fcp.annee_experience * 12 + fcp.mois_experience) BETWEEN 13 AND 24)
                OR (o.niveau_experience = 'Confirmé' AND (fcp.annee_experience * 12 + fcp.mois_experience) BETWEEN 25 AND 48)
                OR (o.niveau_experience = 'Expérimenté' AND (fcp.annee_experience * 12 + fcp.mois_experience) BETWEEN 49 AND 72)
                OR (o.niveau_experience = 'Très expérimenté' AND (fcp.annee_experience * 12 + fcp.mois_experience) > 72)
            )
    """)

    results = cursor.fetchall()

    for row in results:
        cursor.execute("""
            INSERT INTO segmentationclient (
                client_id, nom_client, prenom_client, secteur_client, secteur_id_client,
                offre_emploi, niveau_experience_offre, duree_experience_client
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id)
            DO UPDATE SET
                nom_client = EXCLUDED.nom_client,
                prenom_client = EXCLUDED.prenom_client,
                secteur_client = EXCLUDED.secteur_client,
                secteur_id_client = EXCLUDED.secteur_id_client,
                offre_emploi = EXCLUDED.offre_emploi,
                niveau_experience_offre = EXCLUDED.niveau_experience_offre,
                duree_experience_client = EXCLUDED.duree_experience_client;
        """, row)

    connection.commit()
    cursor.close()
    connection.close()

with DAG(
    dag_id='segmentation_client',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    update_task = PythonOperator(
        task_id='run_segmentation_update',
        python_callable=run_segmentation_update
    )

    update_task
