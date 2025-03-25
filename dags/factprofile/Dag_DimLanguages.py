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

def generate_langue_code(existing_codes):
    valid_codes = [code for code in existing_codes if isinstance(code, str) and code.startswith("LANG")]
    
    if not valid_codes:
        return "LANG001"
    else:
        last_number = max(int(code.replace("LANG", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"LANG{str(new_number).zfill(3)}"

def extract_from_mongodb_to_temp_file(**kwargs):
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.languages": 1, "simpleProfile.languages": 1})

    languages = []
    existing_labels = set()

    for user in mongo_data:
        if isinstance(user, dict):
            if "profile" in user and isinstance(user["profile"], dict):
                language_list = user["profile"].get("languages", [])
                if isinstance(language_list, list):
                    for lang in language_list:
                        if isinstance(lang, dict):
                            label = lang.get("label", "").strip()
                            level = lang.get("level", "").strip()

                            if label and level and label not in existing_labels:
                                existing_labels.add(label)
                                languages.append({
                                    "langue_code": None,
                                    "label": label,
                                    "level": level
                                })
            
            if "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
                language_list = user["simpleProfile"].get("languages", [])
                if isinstance(language_list, list):
                    for lang in language_list:
                        if isinstance(lang, dict):
                            label = lang.get("label", "").strip()
                            level = lang.get("level", "").strip()

                            if label and level and label not in existing_labels:
                                existing_labels.add(label)
                                languages.append({
                                    "langue_code": None,
                                    "label": label,
                                    "level": level
                                })

    client.close()
    
    logger.info(f"Langues extraites: {languages}")

    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        temp_file_path = temp_file.name
        json.dump(languages, temp_file, ensure_ascii=False, indent=4)
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)

def transform_data_from_temp_file(**kwargs):
    temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb_to_temp_file', key='temp_file_path')
    
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT langue_code FROM dim_languages")
    existing_codes = {row[0] for row in cur.fetchall()}
    cur.execute("SELECT label FROM dim_languages")
    existing_labels = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    transformed_data = []
    for record in mongo_data:
        label = record["label"]
        if label and label not in existing_labels:
            langue_code = generate_langue_code(existing_codes)
            transformed_data.append({
                "langue_code": langue_code,
                "label": label,
                "level": record["level"]
            })
            existing_codes.add(langue_code)

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_into_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='transformed_data')

    if not transformed_data:
        logger.info("No data to insert into PostgreSQL.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_languages (langue_code, label, level)
    VALUES (%s, %s, %s)
    ON CONFLICT (langue_code) DO NOTHING;
    """
    update_query = """
    UPDATE dim_languages
    SET level = %s
    WHERE langue_code = %s;
    """
    
    for record in transformed_data:
        cur.execute(insert_query, (record["langue_code"], record["label"], record["level"]))
        if cur.rowcount == 0:
            cur.execute(update_query, (record["level"], record["langue_code"]))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(transformed_data)} rows inserted into PostgreSQL.")

dag = DAG(
    'Dag_DimLanguages',
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
