from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection
def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "secteurdactivities"
        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection established.")
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    return conn

def generate_secteur_code(existing_codes):
    if not existing_codes:
        return "sect0001"
    else:
        last_number = max(int(code.replace("sect", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"sect{str(new_number).zfill(4)}"

def extract_from_mongodb(**kwargs):
    try:
        client, collection = get_mongodb_connection()
        documents = list(collection.find({}, {"label": 1, "_id": 0}))
        labels = [doc['label'].strip().lower() for doc in documents if 'label' in doc]
        client.close()
        kwargs['ti'].xcom_push(key='labels', value=labels)
        logger.info(f"{len(labels)} labels extracted from MongoDB.")
        return labels
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def transform_labels(**kwargs):
    labels = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='labels')
    unique_labels = list(set(labels)) if labels else []
    kwargs['ti'].xcom_push(key='unique_labels', value=unique_labels)
    logger.info(f"{len(unique_labels)} unique labels after transformation.")
    return unique_labels

def load_into_postgres(**kwargs):
    try:
        unique_labels = kwargs['ti'].xcom_pull(task_ids='transform_labels', key='unique_labels')
        if not unique_labels:
            logger.info("No labels to load.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        cur.execute("SELECT secteur_code, label FROM dim_secteur")
        existing = cur.fetchall()
        existing_labels = {row[1]: row[0] for row in existing}

        insert_query = """
            INSERT INTO dim_secteur (secteur_code, label)
            VALUES (%s, %s)
            ON CONFLICT (label)
            DO UPDATE SET secteur_code = EXCLUDED.secteur_code;
        """

        current_codes = list(existing_labels.values())

        for label in unique_labels:
            if label in existing_labels:
                secteur_code = existing_labels[label]
            else:
                secteur_code = generate_secteur_code(current_codes)
                current_codes.append(secteur_code)
            cur.execute(insert_query, (secteur_code, label))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(unique_labels)} labels inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise

dag = DAG(
    dag_id='dag_dim_secteur',
    schedule_interval='*/2 * * * *',
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
