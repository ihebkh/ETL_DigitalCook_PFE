import re
import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    collection = db["frontusers"]
    return client, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion à PostgreSQL réussie.")
    return conn

def get_existing_interests_data():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT nom_interet, interet_id FROM Dim_interests")
    existing_data = cur.fetchall()
    existing_interests = {row[0] for row in existing_data}
    existing_codes = {row[1] for row in existing_data}

    cur.execute("SELECT COALESCE(MAX(interet_id), 0) FROM Dim_interests")
    current_pk = cur.fetchone()[0]

    cur.close()
    conn.close()

    return existing_interests, existing_codes, current_pk

def extract_interests(**kwargs):
    client, collection = get_mongodb_connection()
    cursor = collection.find({}, {
        "_id": 0,
        "profile.interests": 1,
        "simpleProfile.interests": 1
    })

    interests = set()

    for user in cursor:
        for profile_type in ['profile', 'simpleProfile']:
            if profile_type in user and isinstance(user[profile_type], dict):
                entries = user[profile_type].get('interests', [])
                for interest in entries:
                    if isinstance(interest, str) and interest.strip():
                        interests.add(interest.strip())

    client.close()
    kwargs['ti'].xcom_push(key='extracted_interests', value=list(interests))
    logger.info(f"{len(interests)} intérêts extraits.")

def generate_interest_code(existing_codes):
    valid_codes = [code for code in existing_codes if re.match(r"^INT\d{3}$", code)]
    if not valid_codes:
        return "INT001"
    max_num = max(int(code[3:]) for code in valid_codes)
    return f"INT{str(max_num + 1).zfill(3)}"

def transform_interests(**kwargs):
    extracted = kwargs['ti'].xcom_pull(task_ids='extract_interests', key='extracted_interests')
    existing_interests, existing_codes, current_pk = get_existing_interests_data()

    transformed = []
    for interest in extracted:
        if interest not in existing_interests:
            current_pk += 1
            code = generate_interest_code(existing_codes)
            transformed.append({
                "interests_pk": current_pk,
                "interestsCode": code,
                "interests": interest
            })
            existing_codes.add(code)
            existing_interests.add(interest)

    kwargs['ti'].xcom_push(key='transformed_interests', value=transformed)
    logger.info(f"{len(transformed)} nouveaux intérêts préparés.")

def load_interests(**kwargs):
    records = kwargs['ti'].xcom_pull(task_ids='transform_interests', key='transformed_interests')
    if not records:
        logger.info("Aucun intérêt à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO Dim_interests (interet_id, code_interet, nom_interet)
    VALUES (%s, %s, %s)
    ON CONFLICT (interet_id) DO UPDATE SET
    nom_interet = EXCLUDED.nom_interet,
    code_interet = EXCLUDED.code_interet
    """

    for r in records:
        cur.execute(insert_query, (r["interests_pk"], r["interestsCode"], r["interests"]))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(records)} lignes insérées ou mises à jour.")

dag = DAG(
    dag_id='dag_dim_Interests',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_interests',
    python_callable=extract_interests,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_interests',
    python_callable=transform_interests,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_interests',
    python_callable=load_interests,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
