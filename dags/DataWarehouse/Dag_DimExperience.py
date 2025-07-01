import logging
import re
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    mongo_uri = Variable.get("MONGO_URI")
    mongo_db_name = "PowerBi"
    mongo_collection_name = "frontusers"

    client = MongoClient(mongo_uri)
    mongo_db = client[mongo_db_name]
    collection = mongo_db[mongo_collection_name]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def generate_code_experience(pk):
    return f"EXPR{pk:04d}"

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def get_max_experience_pk(cursor):
    cursor.execute("SELECT COALESCE(MAX(experience_id), 0) FROM dim_experience;")
    return cursor.fetchone()[0]

def load_existing_experiences(cursor):
    cursor.execute("""
        SELECT experience_id, role_experience, type_contrat
        FROM public.dim_experience
    """)
    rows = cursor.fetchall()
    mapping = {}
    for row in rows:
        key = (
            str(row[1] or '').lower(),
            str(row[2] or '').lower()
        )
        mapping[key] = row[0]
    return mapping

def extract_experiences(**kwargs):
    client, _, collection = get_mongodb_connection()
    conn = get_postgres_connection()
    with conn:
        with conn.cursor() as cursor:
            existing_experiences = load_existing_experiences(cursor)
            current_pk = get_max_experience_pk(cursor)

            documents = collection.find()
            filtered_experiences = []
            seen_experiences = set()

            for doc in documents:
                for profile_field in ['profile', 'simpleProfile']:
                    if profile_field in doc and isinstance(doc[profile_field], dict):
                        experiences = doc[profile_field].get('experiences')
                        if isinstance(experiences, list):
                            for experience in experiences:
                                if isinstance(experience, dict):
                                    role = experience.get("role", "") or experience.get("poste", "")
                                    type_contrat = experience.get("typeContrat", {}).get("value", "") if isinstance(experience.get("typeContrat", {}), dict) else experience.get("typeContrat", "")

                                    if not role and not type_contrat:
                                        continue

                                    # Removed start_date and end_date related fields
                                    experience_key = (
                                        str(role or '').lower(),
                                        str(type_contrat or '').lower()
                                    )
                                    if experience_key in seen_experiences:
                                        continue

                                    seen_experiences.add(experience_key)
                                    if experience_key in existing_experiences:
                                        experience_pk = existing_experiences[experience_key]
                                    else:
                                        current_pk += 1
                                        experience_pk = current_pk

                                    # Removed start_date and end_date logic
                                    filtered_experience = {
                                        "role": role,
                                        "typeContrat": type_contrat,
                                        "experience_pk": experience_pk,
                                        "code_experience": generate_code_experience(experience_pk)
                                    }

                                    filtered_experiences.append(convert_bson(filtered_experience))
                        else:
                            logger.warning(f"Aucune expérience valide dans '{profile_field}' pour user {doc.get('_id')}")
    client.close()
    conn.close()
    kwargs['ti'].xcom_push(key='dim_experiences', value=filtered_experiences)
    logger.info(f"{len(filtered_experiences)} expériences extraites.")
    return filtered_experiences

def insert_experiences_into_postgres(**kwargs):
    experiences = kwargs['ti'].xcom_pull(task_ids='extract_dim_experience', key='dim_experiences')
    hook = PostgresHook(postgres_conn_id='postgres')
    upsert_query = """
        INSERT INTO public.dim_experience (
            experience_id, code_experience, role_experience,
            type_contrat
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (experience_id) DO UPDATE SET
            code_experience = EXCLUDED.code_experience,
            role_experience = EXCLUDED.role_experience,
            type_contrat = EXCLUDED.type_contrat
        WHERE (
            dim_experience.role_experience IS DISTINCT FROM EXCLUDED.role_experience OR
            dim_experience.type_contrat IS DISTINCT FROM EXCLUDED.type_contrat
        );
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for exp in experiences:
                cursor.execute(upsert_query, (
                    exp["experience_pk"],
                    exp["code_experience"],
                    exp["role"],
                    exp["typeContrat"]
                ))
            conn.commit()
    logger.info(f"{len(experiences)} expériences insérées ou mises à jour.")

dag = DAG(
    dag_id='dag_dim_experience',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting experience extraction process..."),
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_dim_experience',
    python_callable=extract_experiences,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_dim_experience',
    python_callable=insert_experiences_into_postgres,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Experience extraction process completed."),
    dag=dag
)

start_task >> extract_task >> load_task >> end_task
