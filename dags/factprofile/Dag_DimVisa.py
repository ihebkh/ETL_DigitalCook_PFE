from pymongo import MongoClient
from datetime import datetime
import logging
from bson import ObjectId
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

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

def get_max_visa_id():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(visa_id), 0) FROM dim_visa")
    max_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_id

def get_existing_visa_keys():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            LOWER(COALESCE(type_visa, '')),
            date_entree_visa,
            date_sortie_visa,
            LOWER(COALESCE(destination_visa, '')),
            LOWER(COALESCE(nombre_entrees_visa, ''))
        FROM dim_visa
    """)
    existing_keys = {tuple(row) for row in cur.fetchall()}
    cur.close()
    conn.close()
    return existing_keys

def generate_visa_ids(transformed_data):
    max_id = get_max_visa_id()
    for i, record in enumerate(transformed_data, start=max_id + 1):
        record["visa_pk"] = i
        record["visa_code"] = f"VISA{str(i).zfill(4)}"
    return transformed_data

def extract_from_mongodb(**kwargs):
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = list(collection.find({}, {"_id": 0}))
        mongo_data = [convert_datetime_and_objectid_to_string(record) for record in mongo_data]
        client.close()
        kwargs['ti'].xcom_push(key='mongo_data', value=mongo_data)
        logger.info(f"Successfully extracted {len(mongo_data)} records from MongoDB")
        return mongo_data
    except Exception as e:
        logger.error(f"Error in extract_from_mongodb: {str(e)}")
        raise

def transform_data(**kwargs):
    try:
        mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
        transformed_data = []
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
                    transformed_data.append({
                        "visa_pk": None, 
                        "visa_code": None, 
                        "visa_type": visa.get("type", "").strip() or None,
                        "date_entree": visa.get("dateEntree"),
                        "date_sortie": visa.get("dateSortie"),
                        "destination": visa.get("destination", "").strip() or None,
                        "nb_entree": visa.get("nbEntree", "").strip() or None
                    })
        transformed_data = generate_visa_ids(transformed_data)
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
        logger.info(f"Successfully transformed {len(transformed_data)} records")
        return transformed_data
    except Exception as e:
        logger.error(f"Error in transform_data: {str(e)}")
        raise

def get_date_map(cursor):
    cursor.execute("SELECT code_date, date_id FROM public.dim_dates;")
    return {str(code): date_id for code, date_id in cursor.fetchall()}

def load_into_postgres(**kwargs):
    try:
        transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
        if not transformed_data:
            logger.warning("No data to load")
            return
        
        conn = get_postgres_connection()
        cur = conn.cursor()
        date_map = get_date_map(cur)
        
        insert_query = """
        INSERT INTO dim_visa (
            visa_id, code_visa, type_visa, date_entree_visa, date_sortie_visa,
            destination_visa, nombre_entrees_visa
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (visa_id) DO UPDATE SET
            code_visa = EXCLUDED.code_visa,
            type_visa = EXCLUDED.type_visa,
            date_entree_visa = EXCLUDED.date_entree_visa,
            date_sortie_visa = EXCLUDED.date_sortie_visa,
            destination_visa = EXCLUDED.destination_visa,
            nombre_entrees_visa = EXCLUDED.nombre_entrees_visa
        """
        
        records_inserted = 0
        for record in transformed_data:
            date_entree = record["date_entree"]
            date_sortie = record["date_sortie"]
            date_entree_id = date_map.get(str(date_entree)[:10], None) if date_entree else None
            date_sortie_id = date_map.get(str(date_sortie)[:10], None) if date_sortie else None
            
            values = (
                record["visa_pk"],
                record["visa_code"],
                record["visa_type"],
                date_entree_id,
                date_sortie_id,
                record["destination"],
                record["nb_entree"]
            )
            
            try:
                cur.execute(insert_query, values)
                records_inserted += 1
            except Exception as e:
                logger.error(f"Error inserting record: {values}")
                logger.error(f"Error details: {str(e)}")
                raise
        
        conn.commit()
        logger.info(f"Successfully inserted/updated {records_inserted} records")
        
    except Exception as e:
        logger.error(f"Error in load_into_postgres: {str(e)}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

dag = DAG(
    'dag_dim_visa',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, 
    catchup=False
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting visa dimension extraction process..."),
    dag=dag
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

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Visa dimension extraction process completed."),
    dag=dag
)

start_task >> extract_task >> transform_task >> load_task >> end_task