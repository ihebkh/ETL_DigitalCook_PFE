from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_mongodb_connection():
    try:
        client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
        db = client["PowerBi"]
        collection = db["secteurdactivities"]
        logger.info("MongoDB connection established.")
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()


def get_max_secteur_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(secteur_id), 0) FROM dim_secteur")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk

def get_existing_secteur_labels_and_codes():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT code_secteur, nom_secteur FROM dim_secteur")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {row[1]: row[0] for row in rows}

def generate_secteur_code(existing_codes):
    if not existing_codes:
        return "sect0001"
    last_number = max(int(code.replace("sect", "")) for code in existing_codes if code.startswith("sect"))
    return f"sect{str(last_number + 1).zfill(4)}"


def extract_from_mongodb(**kwargs):
    try:
        client, collection = get_mongodb_connection()
        documents = list(collection.find({}, {"label": 1, "_id": 0}))
        labels = [doc['label'].strip().lower() for doc in documents if 'label' in doc]
        client.close()
        kwargs['ti'].xcom_push(key='labels', value=labels)
        logger.info(f"{len(labels)} labels extracted from MongoDB.")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def transform_labels(**kwargs):
    labels = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='labels')
    if not labels:
        logger.info("No labels to transform.")
        return

    unique_labels = list(set(labels))
    max_pk = get_max_secteur_pk()

    transformed = []
    for i, label in enumerate(unique_labels, start=1):
        pk = max_pk + i
        code = f"sect{str(pk).zfill(4)}"
        transformed.append({
            "secteur_pk": pk,
            "secteur_code": code,
            "label": label
        })

    kwargs['ti'].xcom_push(key='transformed_secteurs', value=transformed)
    logger.info(f"{len(transformed)} labels transformed.")
    return transformed

def load_into_postgres(**kwargs):
    try:
        transformed = kwargs['ti'].xcom_pull(task_ids='transform_labels', key='transformed_secteurs')
        if not transformed:
            logger.info("No data to load into PostgreSQL.")
            return

        existing_labels = get_existing_secteur_labels_and_codes()
        current_codes = list(existing_labels.values())

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_secteur (secteur_id, code_secteur, nom_secteur)
        VALUES (%s, %s, %s)
        ON CONFLICT (nom_secteur) DO UPDATE SET
        code_secteur = EXCLUDED.code_secteur,
        nom_secteur = EXCLUDED.nom_secteur;
        """

        for row in transformed:
            if row["label"] in existing_labels:
                row["secteur_code"] = existing_labels[row["label"]]
            elif row["secteur_code"] in current_codes:
                row["secteur_code"] = generate_secteur_code(current_codes)
            current_codes.append(row["secteur_code"])

            cur.execute(insert_query, (
                row["secteur_pk"],
                row["secteur_code"],
                row["label"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(transformed)} labels inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise


dag = DAG(
    dag_id='dag_dim_secteur',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_labels',
    python_callable=transform_labels,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
