import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    collection = db["frontusers"]
    logger.info("Connexion à MongoDB réussie.")
    return client, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion à PostgreSQL réussie.")
    return conn

def get_max_client_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(client_pk), 0) FROM dim_client")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk

def convert_bson(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, dict):
        return {k: convert_bson(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_bson(v) for v in value]
    return value

def extract_data(**kwargs):
    client, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))
    client.close()
    
    clean_data = [convert_bson(doc) for doc in mongo_data]
    kwargs['ti'].xcom_push(key='mongo_data', value=clean_data)
    logger.info(f"{len(clean_data)} documents extraits de MongoDB.")

def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='mongo_data')
    seen_matricules = set()
    transformed = []

    max_pk = get_max_client_pk()

    for record in raw_data:
        matricule = record.get("matricule")
        if not matricule or matricule in seen_matricules:
            continue
        seen_matricules.add(matricule)
        max_pk += 1

        transformed.append({
            "client_pk": max_pk,
            "matricule": matricule,
            "nom": record.get("nom"),
            "prenom": record.get("prenom"),
            "birthdate": record.get("profile", {}).get("birthDate"),
            "nationality": record.get("profile", {}).get("nationality"),
            "adresseDomicile": record.get("profile", {}).get("adresseDomicile"),
            "pays": record.get("profile", {}).get("pays"),
            "situation": record.get("profile", {}).get("situation"),
            "etatcivile": record.get("profile", {}).get("etatCivil"),
            "photo": record.get("google_Photo", record.get("profile", {}).get("google_Photo")),
            "intituleposte": record.get("profile", {}).get("intituleposte"),
            "niveau_etude_actuelle": record.get("profile", {}).get("niveau_etude_actuelle")
        })

    kwargs['ti'].xcom_push(key='transformed_clients', value=transformed)
    logger.info(f"{len(transformed)} clients transformés.")

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_clients')

    if not data:
        logger.warning("Aucune donnée à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_client (
        client_pk, matricule, nom, prenom, birthdate, nationality, adresseDomicile,
        pays, situation, etatcivile, photo, intituleposte, niveau_etude_actuelle
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (matricule) DO UPDATE SET
        nom = EXCLUDED.nom,
        prenom = EXCLUDED.prenom,
        birthdate = EXCLUDED.birthdate,
        nationality = EXCLUDED.nationality,
        adresseDomicile = EXCLUDED.adresseDomicile,
        pays = EXCLUDED.pays,
        situation = EXCLUDED.situation,
        etatcivile = EXCLUDED.etatcivile,
        photo = EXCLUDED.photo,
        intituleposte = EXCLUDED.intituleposte,
        niveau_etude_actuelle = EXCLUDED.niveau_etude_actuelle
    """

    for row in data:
        cur.execute(insert_query, (
            row["client_pk"],
            row["matricule"],
            row.get("nom"),
            row.get("prenom"),
            row.get("birthdate"),
            row.get("nationality"),
            row.get("adresseDomicile"),
            row.get("pays"),
            row.get("situation"),
            row.get("etatcivile"),
            row.get("photo"),
            row.get("intituleposte"),
            row.get("niveau_etude_actuelle")
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} clients insérés/mis à jour dans PostgreSQL.")

# DAG definition
dag = DAG(
    dag_id='Dag_DimClients',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task
