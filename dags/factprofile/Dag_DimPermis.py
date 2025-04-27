from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import logging
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
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def get_max_permis_pk_and_codes():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(permis_id), 0) FROM dim_permis_conduire")
    max_pk = cur.fetchone()[0]

    cur.execute("SELECT categorie_permis FROM dim_permis_conduire")
    existing_categories = {row[0] for row in cur.fetchall()}

    cur.close()
    conn.close()
    return max_pk, existing_categories

def convert_non_serializable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, dict):
        return {key: convert_non_serializable(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [convert_non_serializable(item) for item in value]
    return value

def extract_from_mongodb(**kwargs):
    try:
        client, collection = get_mongodb_connection()
        mongo_data = list(collection.find({}, {"_id": 0}))
        mongo_data = [convert_non_serializable(record) for record in mongo_data]

        client.close()
        kwargs['ti'].xcom_push(key='mongo_data', value=mongo_data)
        return mongo_data
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def transform_data(**kwargs):
    mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
    seen_categories = set()
    transformed_data = []

    max_pk, existing_categories = get_max_permis_pk_and_codes()
    compteur = max_pk

    for record in mongo_data:
        permis_list = []

        if "profile" in record and "permisConduire" in record["profile"]:
            permis_list.extend(record["profile"]["permisConduire"])

        if "simpleProfile" in record and "permisConduire" in record["simpleProfile"]:
            permis_list.extend(record["simpleProfile"]["permisConduire"])

        for permis in permis_list:
            category = permis.strip()
            if category and category not in seen_categories and category not in existing_categories:
                compteur += 1
                code = f"code{str(compteur).zfill(3)}"
                transformed_data.append({
                    "permis_pk": compteur,
                    "permis_code": code,
                    "categorie": category
                })
                seen_categories.add(category)

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    logger.info(f"{len(transformed_data)} nouveaux permis transformés.")
    return transformed_data

def load_into_postgres(**kwargs):
    try:
        transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

        if not transformed_data:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_permis_conduire (permis_id, code_permis, categorie_permis)
        VALUES (%s, %s, %s)
        ON CONFLICT (categorie_permis) DO UPDATE SET
            code_permis = EXCLUDED.code_permis,
            categorie_permis = EXCLUDED.categorie_permis
        """

        for record in transformed_data:
            cur.execute(insert_query, (
                record["permis_pk"],
                record["permis_code"],
                record["categorie"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(transformed_data)} permis records inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

dag = DAG(
    'dag_dim_permis_conduire',
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
    task_id='transform_data',
    python_callable=transform_data,
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
