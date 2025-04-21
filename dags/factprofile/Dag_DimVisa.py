from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
from bson import ObjectId

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

def get_max_visa_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(visa_pk), 0) FROM dim_visa")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk

def convert_datetime_and_objectid_to_string(value):
    if isinstance(value, datetime):
        return value.isoformat() 
    elif isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, dict):
        return {key: convert_datetime_and_objectid_to_string(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [convert_datetime_and_objectid_to_string(item) for item in value]
    return value

def extract_from_mongodb(**kwargs):
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = list(collection.find({}, {"_id": 0}))
        mongo_data = [convert_datetime_and_objectid_to_string(record) for record in mongo_data]
        client.close()
        kwargs['ti'].xcom_push(key='mongo_data', value=mongo_data)
        logger.info("Data extracted from MongoDB successfully.")
        return mongo_data
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def transform_data(**kwargs):
    mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
    transformed_data = []
    max_pk = get_max_visa_pk()
    compteur = max_pk

    for record in mongo_data:
        profiles = [record.get("profile", {})]
        simple_profile = record.get("simpleProfile", {})
        if simple_profile:
            profiles.append(simple_profile)

        for profile_item in profiles:
            visas = profile_item.get("visa", [])
            for visa in visas:
                if not visa:
                    continue
                compteur += 1
                transformed_data.append({
                    "visa_pk": compteur,
                    "visa_code": f"VISA{str(compteur).zfill(4)}",
                    "visa_type": visa.get("type", "").strip() or None,
                    "date_entree": visa.get("dateEntree"),
                    "date_sortie": visa.get("dateSortie"),
                    "destination": visa.get("destination", "").strip() or None,
                    "duree": visa.get("dureeValidite", {}).get("duree", 0),
                    "duree_type": visa.get("dureeValidite", {}).get("type", "").strip() or None,
                    "nb_entree": visa.get("nbEntree", "").strip() or None
                })

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    logger.info(f"{len(transformed_data)} visas transformed.")
    return transformed_data

def load_into_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

    if not transformed_data:
        logger.info("No data to insert into PostgreSQL.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_visa (
        visa_pk, visacode, visa_type, date_entree, date_sortie,
        destination, duree, duree_type, nb_entree
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (visacode) DO UPDATE SET 
        visa_type = EXCLUDED.visa_type,
        date_entree = EXCLUDED.date_entree,
        date_sortie = EXCLUDED.date_sortie,
        destination = EXCLUDED.destination,
        duree = EXCLUDED.duree,
        duree_type = EXCLUDED.duree_type,
        nb_entree = EXCLUDED.nb_entree
    """

    for record in transformed_data:
        values = (
            record["visa_pk"],
            record["visa_code"],
            record["visa_type"],
            record["date_entree"],
            record["date_sortie"],
            record["destination"],
            record["duree"],
            record["duree_type"],
            record["nb_entree"]
        )
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(transformed_data)} visa records inserted/updated in PostgreSQL.")

dag = DAG(
    'dag_dim_visa',
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
