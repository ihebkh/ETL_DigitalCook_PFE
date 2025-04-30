import logging
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION_1 = "frontusers"
        MONGO_COLLECTION_2 = "dossiers"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection_1 = mongo_db[MONGO_COLLECTION_1]
        collection_2 = mongo_db[MONGO_COLLECTION_2]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection_1, collection_2
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def generate_location_code(cursor, existing_codes):
    if not existing_codes: 
        return "LOC001"
    
    cursor.execute("SELECT MAX(CAST(SUBSTRING(code_ville FROM 4) AS INTEGER)) FROM public.dim_ville WHERE code_ville LIKE 'LOC%'")
    max_code_number = cursor.fetchone()[0]
    

    if max_code_number is None:
        return "LOC001"
    
    new_number = max_code_number + 1
    return f"LOC{str(new_number).zfill(3)}"

def handle_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")


def get_existing_villes(cursor):
    cursor.execute("SELECT nom_ville FROM public.dim_ville")
    existing_entries = {row[0] for row in cursor.fetchall()}
    return existing_entries

def extract_villes_and_destinations(**kwargs):
    client, _, collection_1, collection_2 = get_mongodb_connection()

    villes = set()

    frontusers_data = collection_1.find({}, {"_id": 0, "profile.preferedJobLocations": 1, "simpleProfile.preferedJobLocations": 1})
    for record in frontusers_data:
        for profile_key in ["profile", "simpleProfile"]:
            if profile_key in record and "preferedJobLocations" in record[profile_key]:
                for location in record[profile_key]["preferedJobLocations"]:
                    if isinstance(location, dict) and "ville" in location:
                        villes.add(location["ville"])
    destinations = set()
    dossiers_data = collection_2.find({}, {"_id": 0, "firstStep.destination": 1})
    for record in dossiers_data:
        if "firstStep" in record and "destination" in record["firstStep"]:
            for destination in record["firstStep"]["destination"]:
                if isinstance(destination, str):
                    destinations.add(destination)

    villes = list(villes)
    destinations = list(destinations)
    client.close()

    kwargs['ti'].xcom_push(key='villes', value=villes)
    kwargs['ti'].xcom_push(key='destinations', value=destinations)

    logger.info(f"{len(villes)} villes extraites.")
    logger.info(f"{len(destinations)} destinations extraites.")

def load_villes_and_destinations_postgres(**kwargs):
    villes = kwargs['ti'].xcom_pull(task_ids='extract_villes_and_destinations', key='villes')
    destinations = kwargs['ti'].xcom_pull(task_ids='extract_villes_and_destinations', key='destinations')

    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO public.dim_ville (ville_id, code_ville, nom_ville)
    VALUES (%s, %s, %s)
    ON CONFLICT (nom_ville)
    DO UPDATE SET
        code_ville = EXCLUDED.code_ville,
        nom_ville = EXCLUDED.nom_ville;
    """

    existing_entries = get_existing_villes(cursor)

    pk_counter = 1
    inserted_count = 0

    for ville in villes:
        if ville not in existing_entries:
            code = generate_location_code(cursor, existing_entries)
            cursor.execute(insert_query, (pk_counter, code, ville))
            inserted_count += 1
            pk_counter += 1
            existing_entries.add(ville)

    for destination in destinations:
        if destination not in existing_entries:
            code = generate_location_code(cursor, existing_entries)
            cursor.execute(insert_query, (pk_counter, code, destination))
            inserted_count += 1
            pk_counter += 1
            existing_entries.add(destination)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"{inserted_count} villes et destinations insérées ou mises à jour dans PostgreSQL.")

dag = DAG(
    'dag_dim_villes',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_villes_and_destinations',
    python_callable=extract_villes_and_destinations,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_villes_and_destinations_postgres',
    python_callable=load_villes_and_destinations_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task
