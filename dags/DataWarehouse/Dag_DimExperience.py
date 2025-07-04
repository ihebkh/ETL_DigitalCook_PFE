import logging
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["PowerBi"]
    return client, db["frontusers"]

def get_postgres_connection():
    return PostgresHook(postgres_conn_id='postgres').get_conn()


def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_code_experience(pk):
    return f"EXPR{pk:04d}"


def extract_experiences_from_mongo(**kwargs):
    client, collection = get_mongodb_connection()
    documents = collection.find()
    all_experiences = []

    for doc in documents:
        for profile_field in ['profile', 'simpleProfile']:
            profile = doc.get(profile_field, {})
            experiences = profile.get('experiences', [])
            if isinstance(experiences, list):
                for exp in experiences:
                    role = exp.get("role") or exp.get("poste", "")
                    contrat = exp.get("typeContrat", {}).get("value") if isinstance(exp.get("typeContrat", {}), dict) else exp.get("typeContrat", "")
                    all_experiences.append({
                        "role": role.strip() if role else "",
                        "type_contrat": contrat.strip() if contrat else ""
                    })

    client.close()
    kwargs['ti'].xcom_push(key="raw_experiences", value=convert_bson(all_experiences))
    logger.info(f"{len(all_experiences)} expériences brutes extraites")

def transform_experiences(**kwargs):
    raw = kwargs['ti'].xcom_pull(task_ids='extract_experiences_from_mongo', key='raw_experiences')
    conn = get_postgres_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COALESCE(MAX(experience_id), 0) FROM dim_experience")
            current_pk = cursor.fetchone()[0]

            cursor.execute("SELECT role_experience, type_contrat, experience_id FROM dim_experience")
            existing = {(r.lower(), c.lower()): i for r, c, i in cursor.fetchall()}

            seen = set()
            transformed = []
            for exp in raw:
                key = (exp['role'].lower(), exp['type_contrat'].lower())
                if key in seen:
                    continue
                seen.add(key)

                if key in existing:
                    pk = existing[key]
                else:
                    current_pk += 1
                    pk = current_pk

                transformed.append({
                    "experience_id": pk,
                    "code_experience": generate_code_experience(pk),
                    "role_experience": exp["role"],
                    "type_contrat": exp["type_contrat"]
                })

    conn.close()
    kwargs['ti'].xcom_push(key="transformed_experiences", value=transformed)
    logger.info(f"{len(transformed)} expériences transformées")

def load_experiences_to_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_experiences', key='transformed_experiences')
    if not data:
        logger.info("Aucune expérience à insérer.")
        return

    conn = get_postgres_connection()
    with conn:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_experience (experience_id, code_experience, role_experience, type_contrat)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (experience_id) DO UPDATE SET
                    code_experience = EXCLUDED.code_experience,
                    role_experience = EXCLUDED.role_experience,
                    type_contrat = EXCLUDED.type_contrat
                WHERE 
                    dim_experience.role_experience IS DISTINCT FROM EXCLUDED.role_experience OR
                    dim_experience.type_contrat IS DISTINCT FROM EXCLUDED.type_contrat;
            """
            for exp in data:
                cursor.execute(insert_query, (
                    exp["experience_id"], exp["code_experience"], exp["role_experience"], exp["type_contrat"]
                ))
            conn.commit()
    conn.close()
    logger.info(f"{len(data)} lignes insérées ou mises à jour dans dim_experience")

dag = DAG(
    dag_id='dag_dim_experience',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Début du traitement des expériences."),
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_experiences_from_mongo',
    python_callable=extract_experiences_from_mongo,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_experiences',
    python_callable=transform_experiences,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_experiences_to_postgres',
    python_callable=load_experiences_to_postgres,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Fin du traitement des expériences."),
    dag=dag
)

start_task >> extract_task >> transform_task >> load_task >> end_task
