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
    existing_keys = get_existing_visa_keys()
    new_records = []
    for record in transformed_data:
        key = (
            record["visa_type"].lower() if record["visa_type"] else '',
            record["date_entree"],
            record["date_sortie"],
            record["destination"].lower() if record["destination"] else '',
            record["nb_entree"].lower() if record["nb_entree"] else ''
        )
        if key not in existing_keys:
            new_records.append(record)
    
    # Générer les IDs pour les nouveaux enregistrements
    for i, record in enumerate(new_records, start=1):
        record["visa_pk"] = i
        record["visa_code"] = f"VISA{str(i).zfill(4)}"
    
    return new_records

def extract_from_mongodb(**kwargs):
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))
    mongo_data = [convert_datetime_and_objectid_to_string(record) for record in mongo_data]
    client.close()
    kwargs['ti'].xcom_push(key='mongo_data', value=mongo_data)
    return mongo_data

def transform_data(**kwargs):
    mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
    transformed_data = []
    
    for record in mongo_data:
        profile = record.get("profile", {})
        if not profile:
            continue
            
        visas = profile.get("visa", [])
        for visa in visas:
            if not visa:
                continue
                
            # Gestion des dates
            date_entree = None
            date_sortie = None
            
            # Vérifier si dateEntree est un dictionnaire ou une chaîne
            date_entree_raw = visa.get("dateEntree")
            if isinstance(date_entree_raw, dict):
                date_entree = date_entree_raw.get("$date")
            elif isinstance(date_entree_raw, str):
                date_entree = date_entree_raw
                
            # Vérifier si dateSortie est un dictionnaire ou une chaîne
            date_sortie_raw = visa.get("dateSortie")
            if isinstance(date_sortie_raw, dict):
                date_sortie = date_sortie_raw.get("$date")
            elif isinstance(date_sortie_raw, str):
                date_sortie = date_sortie_raw
            
            # Conversion des dates en format datetime si nécessaire
            if isinstance(date_entree, str):
                try:
                    date_entree = datetime.fromisoformat(date_entree.replace('Z', '+00:00'))
                except ValueError:
                    date_entree = None
                    
            if isinstance(date_sortie, str):
                try:
                    date_sortie = datetime.fromisoformat(date_sortie.replace('Z', '+00:00'))
                except ValueError:
                    date_sortie = None
            
            # Convertir les dates en chaînes de caractères pour la sérialisation
            date_entree_str = date_entree.isoformat() if date_entree else None
            date_sortie_str = date_sortie.isoformat() if date_sortie else None
            
            transformed_data.append({
                "visa_pk": None,
                "visa_code": None,
                "visa_type": visa.get("type", "").strip() or None,
                "date_entree": date_entree_str,
                "date_sortie": date_sortie_str,
                "destination": visa.get("destination", "").strip() or None,
                "nb_entree": visa.get("nbEntree", "").strip() or None
            })
    
    transformed_data = generate_visa_ids(transformed_data)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    return transformed_data

def get_date_map(cursor):
    cursor.execute("SELECT code_date, date_id FROM public.dim_dates;")
    return {str(code): date_id for code, date_id in cursor.fetchall()}

def load_into_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    if not transformed_data:
        logger.info("No new visa data to load")
        return
        
    conn = get_postgres_connection()
    cur = conn.cursor()
    date_map = get_date_map(cur)
    
    insert_query = """
    INSERT INTO dim_visa (
        visa_id, code_visa, type_visa, date_entree_visa, date_sortie_visa,
        destination_visa, nombre_entrees_visa
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        for record in transformed_data:
            date_entree = record["date_entree"]
            date_sortie = record["date_sortie"]
            
            # Convertir les dates en format string pour la recherche dans date_map
            date_entree_str = date_entree[:10] if date_entree else None  # Prendre juste la partie date (YYYY-MM-DD)
            date_sortie_str = date_sortie[:10] if date_sortie else None  # Prendre juste la partie date (YYYY-MM-DD)
            
            date_entree_id = date_map.get(date_entree_str) if date_entree_str else None
            date_sortie_id = date_map.get(date_sortie_str) if date_sortie_str else None
            
            values = (
                record["visa_pk"],
                record["visa_code"],
                record["visa_type"],
                date_entree_id,
                date_sortie_id,
                record["destination"],
                record["nb_entree"]
            )
            cur.execute(insert_query, values)
        
        conn.commit()
        logger.info(f"Successfully loaded {len(transformed_data)} visa records")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading visa data: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

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

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting visa extraction process..."),
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Visa extraction process completed."),
    dag=dag
)

start_task >> extract_task >> transform_task >> load_task >> end_task 