import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
        db = client["PowerBi"]
        collection = db["frontusers"]
        logger.info("MongoDB connection successful.")
        return client, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_max_niveau_pk(conn):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(niveau_etude_id), 0) FROM dim_niveau_d_etudes")
    max_pk = cur.fetchone()[0]
    cur.close()
    return max_pk

def extract_niveau_etudes(**kwargs):
    client, collection = get_mongodb_connection()
    data = []

    for doc in collection.find():
        for etude in doc.get("simpleProfile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            label = etude.get("label", "").strip()
            if label and label.lower() != "null":
                data.append({"label": label})

        for etude in doc.get("profile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            label = etude.get("label", "").strip()
            if label and label.lower() != "null":
                data.append({"label": label})

    kwargs['ti'].xcom_push(key='niveau_raw_data', value=data)
    logger.info(f"{len(data)} niveaux d’études extraits.")

def transform_niveau_etudes(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_dim_niveau_etudes', key='niveau_raw_data')
    if not raw_data:
        logger.info("Aucune donnée à transformer.")
        return

    conn = get_postgres_connection()
    max_pk = get_max_niveau_pk(conn)
    transformed = []
    compteur = 0

    for row in raw_data:
        label = row.get("label")
        if not label or label.lower() == "null":
            continue

        compteur += 1
        max_pk += 1
        code = f"DIP{compteur:03d}"

        transformed.append({
            "niveau_pk": max_pk,
            "code": code,
            "label": label
        })

    kwargs['ti'].xcom_push(key='niveau_transformed', value=transformed)
    logger.info(f"{len(transformed)} lignes transformées avec succès.")

def load_niveau_etudes_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_niveau_etudes', key='niveau_transformed')
    if not data:
        logger.info("Aucune donnée à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_niveau_d_etudes (
        niveau_etude_id, code_diplome, nom_diplome
    ) VALUES (%s, %s, %s)
    ON CONFLICT (nom_diplome)
    DO UPDATE SET
        code_diplome = EXCLUDED.code_diplome,
        nom_diplome = EXCLUDED.nom_diplome;
    """

    for row in data:
        cur.execute(insert_query, (
            row['niveau_pk'],
            row['code'],
            row['label']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} niveaux d’études insérés ou mis à jour.")

with DAG(
    dag_id='dag_dim_niveau_etudes',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_dim_niveau_etudes',
        python_callable=extract_niveau_etudes,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_niveau_etudes',
        python_callable=transform_niveau_etudes,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_dim_niveau_etudes',
        python_callable=load_niveau_etudes_postgres,
        provide_context=True,
    )

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("Starting region extraction process..."),
        dag=dag
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Region extraction process completed."),
        dag=dag
    )

    start_task >> extract_task >> transform_task >> load_task >> end_task
