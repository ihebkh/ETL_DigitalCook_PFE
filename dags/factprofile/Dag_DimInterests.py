import tempfile
import re
import json
import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion à PostgreSQL réussie.")
    return conn

def generate_interests_code(existing_codes):
    valid_codes = [code for code in existing_codes if re.match(r"^INT\d{3}$", code)]
    
    if not valid_codes:
        return "INT001"
    else:
        last_number = max(int(code.replace("INT", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"INT{str(new_number).zfill(3)}"

def extract_from_mongodb_to_temp_file(**kwargs):
    """ Extract data from MongoDB and save it to a temporary file. """
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.interests": 1, "simpleProfile.interests": 1})

    interests = set()

    for user in mongo_data:
        if isinstance(user, dict):
            if "profile" in user and isinstance(user["profile"], dict):
                user_interests = user["profile"].get("interests", [])
                if isinstance(user_interests, list):
                    for interest in user_interests:
                        if isinstance(interest, str) and interest.strip():
                            interests.add(interest.strip())
            if "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
                user_interests = user["simpleProfile"].get("interests", [])
                if isinstance(user_interests, list):
                    for interest in user_interests:
                        if isinstance(interest, str) and interest.strip():
                            interests.add(interest.strip())

    client.close()
    
    logger.info(f"Intérêts extraits : {interests}")

    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        temp_file_path = temp_file.name
        json.dump([{"interestsCode": None, "interests": i} for i in interests], temp_file, ensure_ascii=False, indent=4)
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)

def transform_data_from_temp_file(**kwargs):
    """ Transform the data from the temporary file. """
    temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb_to_temp_file', key='temp_file_path')
    
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT interests FROM Dim_interests")
    existing_interests = {row[0] for row in cur.fetchall()}
    cur.execute("SELECT interestsCode FROM Dim_interests")
    existing_codes = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    transformed_data = []
    for record in mongo_data:
        interest_name = record["interests"]
        if interest_name and interest_name not in existing_interests:
            interest_code = generate_interests_code(existing_codes)
            transformed_data.append({
                "interestsCode": interest_code,
                "interests": interest_name
            })
            existing_codes.add(interest_code)

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_into_postgres(**kwargs):
    """ Load the transformed data into PostgreSQL. """
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='transformed_data')

    if not transformed_data:
        logger.info("No data to insert into PostgreSQL.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO Dim_interests (interestsCode, interests)
    VALUES (%s, %s)
    ON CONFLICT (interestsCode) DO NOTHING;
    """
    update_query = """
    UPDATE Dim_interests
    SET interests = %s
    WHERE interestsCode = %s;
    """
    
    for record in transformed_data:
        cur.execute(insert_query, (record["interestsCode"], record["interests"]))
        if cur.rowcount == 0:
            cur.execute(update_query, (record["interests"], record["interestsCode"]))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(transformed_data)} rows inserted into PostgreSQL.")

dag = DAG(
    'Dag_DimInterests',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb_to_temp_file',
    python_callable=extract_from_mongodb_to_temp_file,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data_from_temp_file',
    python_callable=transform_data_from_temp_file,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
