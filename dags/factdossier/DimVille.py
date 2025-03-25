import logging
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id="postgres")
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["universities"]
    return client, mongo_db, collection

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def extract_villes(**kwargs):
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "ville": 1})

    villes = set()
    for university in universities:
        villes.update(university.get("ville", []))

    villes = list(set(convert_bson(villes)))
    client.close()
    kwargs['ti'].xcom_push(key='villes', value=villes)
    logger.info(f"{len(villes)} villes extraites de MongoDB.")

def generate_city_code(index):
    return f"ville{str(index).zfill(4)}"

def load_villes(**kwargs):
    villes = kwargs['ti'].xcom_pull(task_ids='extract_villes', key='villes')
    if not villes:
        logger.info("Aucune ville à charger.")
        return

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO dim_ville (codeville, nom_ville)
        VALUES (%s, %s)
        ON CONFLICT (codeville) DO UPDATE
        SET nom_ville = EXCLUDED.nom_ville;
    """

    for index, ville in enumerate(villes, start=1):
        code = generate_city_code(index)
        cursor.execute(insert_query, (code, ville))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(villes)} villes insérées/mises à jour.")

dag = DAG(
    dag_id='dag_dim_ville',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_villes',
    python_callable=extract_villes,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_villes',
    python_callable=load_villes,
    provide_context=True,
    dag=dag
)

extract_task >> load_task