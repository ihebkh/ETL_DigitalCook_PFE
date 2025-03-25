from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from bson import ObjectId
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "frontusers"
        
        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def get_next_permis_code():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dim_permis_conduire")
    count = cur.fetchone()[0] + 1
    cur.close()
    conn.close()
    return f"code{str(count).zfill(3)}"

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
        client, _, collection = get_mongodb_connection()
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
    
    for record in mongo_data:
        permis_list = []
        
        if "profile" in record and "permisConduire" in record["profile"]:
            permis_list.extend(record["profile"]["permisConduire"])

        if "simpleProfile" in record and "permisConduire" in record["simpleProfile"]:
            permis_list.extend(record["simpleProfile"]["permisConduire"])

        for permis in permis_list:
            category = permis.strip()
            if category and category not in seen_categories:
                seen_categories.add(category)
                transformed_data.append({
                    "permis_code": get_next_permis_code(),
                    "categorie": category
                })

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
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
        INSERT INTO dim_permis_conduire (permis_code, categorie)
        VALUES (%s, %s)
        ON CONFLICT (categorie) DO UPDATE SET permis_code = EXCLUDED.permis_code
        """

        for record in transformed_data:
            values = (
                record["permis_code"],
                record["categorie"]
            )
            cur.execute(insert_query, values)

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(transformed_data)} permis records inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimPermisConduire',
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
