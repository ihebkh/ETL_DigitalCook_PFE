import logging
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["universities"]
    dossiers_collection = mongo_db["dossiers"]
    return client, mongo_db, collection, dossiers_collection

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_code(index):
    return f"code{str(index).zfill(4)}"

def get_next_ville_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(ville_pk) FROM public.dim_ville;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def extract_villes_and_destinations(**kwargs):
    client, mongo_db, collection, dossiers_collection = get_mongodb_connection()

    universities = collection.find({}, {"_id": 0, "ville": 1})
    villes = set()
    for university in universities:
        villes.update(university.get("ville", []))

    villes = list(set(convert_bson(villes))) 

    destinations = set()
    dossiers = dossiers_collection.find({}, {"_id": 0, "firstStep.destination": 1})
    for record in dossiers:
        if "firstStep" in record and "destination" in record["firstStep"]:
            destinations.update(record["firstStep"]["destination"])

    destinations = list(set(convert_bson(destinations)))
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
    INSERT INTO public.dim_ville (ville_pk, code, name, type)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (ville_pk)
    DO UPDATE SET
        code = EXCLUDED.code,
        name = EXCLUDED.name,
        type = EXCLUDED.type;
    """

    pk_counter = get_next_ville_pk()
    for ville in villes:
        code = generate_code(pk_counter)
        cursor.execute(insert_query, (pk_counter, code, ville, 'Ville'))
        pk_counter += 1
    
    for destination in destinations:
        code = generate_code(pk_counter)
        cursor.execute(insert_query, (pk_counter, code, destination, 'Destination'))
        pk_counter += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"{len(villes)} villes et {len(destinations)} destinations insérées ou mises à jour.")

with DAG(
    dag_id='dag_dim_villes',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_villes_and_destinations',
        python_callable=extract_villes_and_destinations,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_villes_and_destinations_postgres',
        python_callable=load_villes_and_destinations_postgres,
        provide_context=True,
    )

    extract_task >> load_task
