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

def extract_partenaires(**kwargs):
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "partenairesProfessionnel": 1})

    partenaires = []
    for university in universities:
        partenaires.extend(university.get("partenairesProfessionnel", []))

    partenaires = list(set(convert_bson(partenaires)))
    client.close()
    kwargs['ti'].xcom_push(key='partenaires_pro', value=partenaires)
    logger.info(f"{len(partenaires)} partenaires professionnels extraits de MongoDB.")

def generate_partenaire_professional_code(index):
    return f"partenairePro{str(index).zfill(4)}"

def load_partenaires(**kwargs):
    partenaires = kwargs['ti'].xcom_pull(task_ids='extract_partenaires', key='partenaires_pro')
    if not partenaires:
        logger.info("Aucun partenaire professionnel à charger.")
        return

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO dim_partenaire_proessionnel (codepartenaireprofessional, nom_partenaire)
        VALUES (%s, %s)
        ON CONFLICT (codepartenaireprofessional) DO UPDATE
        SET nom_partenaire = EXCLUDED.nom_partenaire;
    """

    for index, partenaire in enumerate(partenaires, start=1):
        code = generate_partenaire_professional_code(index)
        cursor.execute(insert_query, (code, partenaire))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(partenaires)} partenaires professionnels insérés/mis à jour.")

dag = DAG(
    dag_id='dag_dim_partenaire_professionnel',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_partenaires',
    python_callable=extract_partenaires,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_partenaires',
    python_callable=load_partenaires,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
