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

def get_existing_competences():
    """ Fetch existing competencies from the PostgreSQL database. """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT competence_name FROM dim_competence")
    competences = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return competences

def extract_from_mongodb_to_temp_file(**kwargs):
    """ Extract data from MongoDB and save it to a temporary file. """
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0, "profile.experiences.competances": 1, "simpleProfile.experiences.competances": 1}))
    
    competencies = []
    
    for user in mongo_data:
        if "profile" in user and "experiences" in user["profile"]:
            for experience in user["profile"]["experiences"]:
                competencies.extend(experience.get("competances", []))
        if "simpleProfile" in user and "experiences" in user["simpleProfile"]:
            for experience in user["simpleProfile"]["experiences"]:
                competencies.extend(experience.get("competances", []))

    unique_competencies = list(set(competencies))
    
    data_to_save = [{"competance": comp} for comp in unique_competencies]
    
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        json.dump(data_to_save, temp_file, ensure_ascii=False, indent=4)
        temp_file_path = temp_file.name
    
    client.close()
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
    logger.info(f"Data extracted and saved to temporary file: {temp_file_path}")

def transform_data_from_temp_file(**kwargs):
    """ Transform the data from the temporary file. """
    temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb_to_temp_file', key='temp_file_path')
    
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    existing_competences = get_existing_competences()
    transformed_data = []
    competence_code_counter = len(existing_competences)

    for record in mongo_data:
        competence = record.get("competance")
        competence = competence.strip() if competence else None
        if competence and competence not in existing_competences:
            competence_code_counter += 1
            new_competence_code = f"COMP{str(competence_code_counter).zfill(2)}"
            transformed_data.append({
                "competence_code": new_competence_code,
                "competence_name": competence
            })
            existing_competences.add(competence)
    
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    logger.info(f"Data transformed: {len(transformed_data)} new competencies found.")

def load_into_postgres(**kwargs):
    """ Load the transformed data into PostgreSQL. """
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='transformed_data')
    
    if not transformed_data:
        logger.info("No data to insert into PostgreSQL.")
        return
    
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_competence (competence_code, competence_name)
    VALUES (%s, %s)
    ON CONFLICT (competence_code) DO NOTHING;  -- Ensure competence_code is unique
    """
    update_query = """
    UPDATE dim_competence
    SET competence_name = %s
    WHERE competence_code = %s;  -- Update by competence_code
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
    'Dag_DimCompetences',
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