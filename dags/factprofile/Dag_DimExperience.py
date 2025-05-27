import logging
import re
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db["frontusers"]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def parse_date(date_value):
    if isinstance(date_value, dict):
        year = date_value.get("year")
        month = date_value.get("month")
        year = int(year) if isinstance(year, (int, str)) and str(year).isdigit() else None
        month = int(month) if isinstance(month, (int, str)) and str(month).isdigit() else None
        return year, month
    elif isinstance(date_value, str):
        if re.match(r"^\d{4}-\d{2}$", date_value):
            year, month = map(int, date_value.split("-"))
            return year, month
        elif re.match(r"^\d{4}$", date_value):
            return int(date_value), None
    return None, None

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
        SELECT experience_id, role_experience, annee_debut, mois_debut, annee_fin, mois_fin, type_contrat
        FROM public.dim_experience
    """)
    rows = cursor.fetchall()
    mapping = {}
    for row in rows:
        key = (
            str(row[1] or '').lower(),
            str(row[6] or '').lower(),
            str(row[2] or ''),
            str(row[3] or ''),
            str(row[4] or ''),
            str(row[5] or '')
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

                                    start_year, start_month = parse_date(experience.get("du", ""))
                                    end_year, end_month = parse_date(experience.get("au", ""))

                                    experience_key = (
                                        str(role or '').lower(),
                                        str(type_contrat or '').lower(),
                                        str(start_year or ''),
                                        str(start_month or ''),
                                        str(end_year or ''),
                                        str(end_month or '')
                                    )
                                    if experience_key in seen_experiences:
                                        continue

                                    seen_experiences.add(experience_key)
                                    if experience_key in existing_experiences:
                                        experience_pk = existing_experiences[experience_key]
                                    else:
                                        current_pk += 1
                                        experience_pk = current_pk

                                    filtered_experience = {
                                        "role": role,
                                        "typeContrat": type_contrat,
                                        "du_year": start_year,
                                        "du_month": start_month,
                                        "au_year": end_year,
                                        "au_month": end_month,
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
            annee_debut, mois_debut, annee_fin, mois_fin, type_contrat
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (experience_id) DO UPDATE SET
            code_experience = EXCLUDED.code_experience,
            role_experience = EXCLUDED.role_experience,
            annee_debut = EXCLUDED.annee_debut,
            mois_debut = EXCLUDED.mois_debut,
            annee_fin = EXCLUDED.annee_fin,
            mois_fin = EXCLUDED.mois_fin,
            type_contrat = EXCLUDED.type_contrat
        WHERE (
            dim_experience.role_experience IS DISTINCT FROM EXCLUDED.role_experience OR
            dim_experience.type_contrat IS DISTINCT FROM EXCLUDED.type_contrat OR
            dim_experience.annee_debut IS DISTINCT FROM EXCLUDED.annee_debut OR
            dim_experience.mois_debut IS DISTINCT FROM EXCLUDED.mois_debut OR
            dim_experience.annee_fin IS DISTINCT FROM EXCLUDED.annee_fin OR
            dim_experience.mois_fin IS DISTINCT FROM EXCLUDED.mois_fin
        );
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for exp in experiences:
                cursor.execute(upsert_query, (
                    exp["experience_pk"],
                    exp["code_experience"],
                    exp["role"],
                    int(exp["du_year"]) if exp["du_year"] else None,
                    int(exp["du_month"]) if exp["du_month"] else None,
                    int(exp["au_year"]) if exp["au_year"] else None,
                    int(exp["au_month"]) if exp["au_month"] else None,
                    exp["typeContrat"]
                ))
            conn.commit()
    logger.info(f"{len(experiences)} expériences insérées ou mises à jour.")

dag = DAG(
    dag_id='dag_dim_experience',
    start_date=datetime(2025, 1, 1),
    catchup=False
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