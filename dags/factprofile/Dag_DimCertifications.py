import tempfile
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson import ObjectId
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["frontusers"]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def extract_from_mongodb_to_temp_file():
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))

    def datetime_converter(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError("Type non sérialisable : {}".format(type(obj)))

    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        json.dump(mongo_data, temp_file, ensure_ascii=False, indent=4, default=datetime_converter)
        temp_file_path = temp_file.name

    client.close()
    return temp_file_path

def transform_data_from_temp_file(temp_file_path):
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    seen_certifications = set()
    transformed_data = []
    current_certification_code = 1

    for record in mongo_data:
        certifications_list = []

        if "profile" in record and "certifications" in record["profile"]:
            certifications_list.extend(record["profile"]["certifications"])

        if "simpleProfile" in record and "certifications" in record["simpleProfile"]:
            certifications_list.extend(record["simpleProfile"]["certifications"])

        for cert in certifications_list:
            if isinstance(cert, str):
                nomCertification = cert.strip()
                date = None
            elif isinstance(cert, dict):
                nomCertification = cert.get("nomCertification", "").strip()
                date = cert.get("year")
            else:
                continue

            if nomCertification and (nomCertification, date) not in seen_certifications:
                seen_certifications.add((nomCertification, date))
                certification_code = f"certif{str(current_certification_code).zfill(4)}"
                current_certification_code += 1
                transformed_data.append((certification_code, nomCertification, date))

    return transformed_data

def load_into_postgres(data):
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_certification (certificationcode, nom, date)
    VALUES (%s, %s, %s)
    ON CONFLICT (certificationcode) DO UPDATE SET 
        nom = EXCLUDED.nom,
        date = EXCLUDED.date;
    """

    for record in data:
        cur.execute(insert_query, record)

    conn.commit()
    cur.close()
    conn.close()

dag = DAG(
    'Dag_Dimcertiifcations',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb_to_temp_file',
    python_callable=extract_from_mongodb_to_temp_file,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data_from_temp_file',
    python_callable=transform_data_from_temp_file,
    op_args=[extract_task.output],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    op_args=[transform_task.output],
    dag=dag,
)

extract_task >> transform_task >> load_task
