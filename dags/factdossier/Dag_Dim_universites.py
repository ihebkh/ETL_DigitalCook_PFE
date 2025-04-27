import logging
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

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
    return client, collection

def sanitize_for_json(doc):
    if isinstance(doc, dict):
        return {k: sanitize_for_json(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [sanitize_for_json(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc

def generate_codeuniv(counter):
    return f"univ{counter:04d}"

def upsert_fact_universite(universite_pk, codeuniversite, nom_uni, pays):
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute(""" 
        INSERT INTO dim_universite (universite_id, code_universite, nom_universite, pays_universite)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (universite_id) DO UPDATE SET
            code_universite = EXCLUDED.code_universite,  
            nom_universite = EXCLUDED.nom_universite,
            pays_universite = EXCLUDED.pays_universite;
    """, (universite_pk, codeuniversite, nom_uni, pays))
    conn.commit()
    cur.close()
    conn.close()

def extract_universites(**kwargs):
    client, collection = get_mongodb_connection()
    raw_data = list(collection.find({}, {
        "nom": 1, "pays": 1, "created_at": 1, "ville": 1,
        "filiere": 1, "partenairesAcademique": 1,
        "partenairesProfessionnel": 1
    }))
    client.close()
    cleaned_data = [sanitize_for_json(doc) for doc in raw_data]
    kwargs['ti'].xcom_push(key='universites', value=cleaned_data)

def insert_universites(**kwargs):
    universites = kwargs['ti'].xcom_pull(task_ids='extract_universites_task', key='universites')
    if not universites:
        logger.info("Aucune donnée à insérer.")
        return

    universite_code_map = {}
    code_counter = 1
    total = 0
    universite_pk_counter = 1

    for doc in universites:
        nom_uni = doc.get("nom", "Université inconnue")
        nom_uni_cleaned = nom_uni.strip().lower()

        if nom_uni_cleaned not in universite_code_map:
            universite_code_map[nom_uni_cleaned] = generate_codeuniv(code_counter)
            code_counter += 1

        codeuniversite = universite_code_map[nom_uni_cleaned]
        pays = doc.get("pays")
        universite_pk = universite_pk_counter
        upsert_fact_universite(universite_pk, codeuniversite, nom_uni, pays)
        universite_pk_counter += 1
        total += 1

    logger.info(f"Total universités insérées ou mises à jour : {total}")

dag = DAG(
    dag_id='dag_dim_universites',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_universites_task',
    python_callable=extract_universites,
    provide_context=True,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_universites_task',
    python_callable=insert_universites,
    provide_context=True,
    dag=dag
)


extract_task >> insert_task
