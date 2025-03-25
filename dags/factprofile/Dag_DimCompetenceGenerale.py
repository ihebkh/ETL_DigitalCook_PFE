import tempfile
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

def generate_competence_code(existing_codes):
    if not existing_codes:
        return "COMP001"
    else:
        last_number = max(int(code.replace("COMP", "")) for code in existing_codes)
        new_number = last_number + 1
        return f"COMP{str(new_number).zfill(3)}"

def extract_from_mongodb_to_temp_file(**kwargs):
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.competenceGenerales": 1, "simpleProfile.competenceGenerales": 1})

    competences = set()

    for user in mongo_data:
        if isinstance(user, dict):
            if "profile" in user and isinstance(user["profile"], dict):
                user_competences = user["profile"].get("competenceGenerales", [])
                if isinstance(user_competences, list):
                    for competence in user_competences:
                        if isinstance(competence, str) and competence.strip(): 
                            competences.add(competence.strip())

            if "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
                user_competences = user["simpleProfile"].get("competenceGenerales", [])
                if isinstance(user_competences, list):
                    for competence in user_competences:
                        if isinstance(competence, str) and competence.strip(): 
                            competences.add(competence.strip())

    client.close()
    
    logger.info(f"Compétences extraites : {competences}")

    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        temp_file_path = temp_file.name
        json.dump([{"competenceCode": None, "competence_name": c} for c in competences], temp_file, ensure_ascii=False, indent=4)
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)

def transform_data_from_temp_file(**kwargs):
    temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb_to_temp_file', key='temp_file_path')
    
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT competenceCode FROM Dim_competence_generale")
    existing_codes = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    transformed_data = []
    competence_code_counter = len(existing_codes)

    for record in mongo_data:
        competence_name = record["competence_name"]
        if competence_name and competence_name not in existing_codes:
            competence_code_counter += 1
            new_competence_code = generate_competence_code(existing_codes)
            transformed_data.append({
                "competence_code": new_competence_code,
                "competence_name": competence_name
            })
            existing_codes.add(new_competence_code)
    
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_into_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='transformed_data')

    if not transformed_data:
        logger.info("No data to insert into PostgreSQL.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO Dim_competence_generale (competenceCode, competence_name)
    VALUES (%s, %s)
    ON CONFLICT (competence_name) DO NOTHING;
    """
    update_query = """
    UPDATE Dim_competence_generale
    SET competence_name = %s
    WHERE competenceCode = %s;
    """
    
    for record in transformed_data:
        cur.execute(insert_query, (record["competence_code"], record["competence_name"]))
        if cur.rowcount == 0:
            cur.execute(update_query, (record["competence_name"], record["competence_code"]))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(transformed_data)} rows inserted into PostgreSQL.")

dag = DAG(
    'Dag_DimCompetenceGenerale',
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
