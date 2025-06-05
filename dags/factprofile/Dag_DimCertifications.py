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
    MONGO_URI = Variable.get("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return client, db["frontusers"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def extract_from_mongodb(ti):
    client, collection = get_mongodb_connection()
    data = list(collection.find({}, {"_id": 0}))
    client.close()

    cleaned = []
    for doc in data:
        def clean(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: clean(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean(v) for v in value]
            else:
                return value
        cleaned.append(clean(doc))

    ti.xcom_push(key="certifications_raw", value=cleaned)

def transform_certifications(ti):
    raw_data = ti.xcom_pull(task_ids='extract_from_mongodb', key='certifications_raw')
    seen = set()
    result = []
    counter = 1

    for record in raw_data:
        certifs = []

        if "profile" in record and "certifications" in record["profile"]:
            certifs.extend(record["profile"]["certifications"])

        if "simpleProfile" in record and "certifications" in record["simpleProfile"]:
            certifs.extend(record["simpleProfile"]["certifications"])

        for cert in certifs:
            if isinstance(cert, str):
                name = cert.strip()
            elif isinstance(cert, dict):
                name = cert.get("nomCertification", "").strip()
            else:
                continue

            if name and name.lower() not in seen:
                code = f"certif{str(counter).zfill(4)}"
                result.append((counter, code, name))
                seen.add(name.lower())
                counter += 1

    ti.xcom_push(key='certifications_transformed', value=result)

def load_into_postgres(ti):
    data = ti.xcom_pull(task_ids='transform_certifications', key='certifications_transformed')
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_certification (certification_id, code_certification, nom_certification)
    VALUES (%s, %s, %s)
    ON CONFLICT (certification_id) DO UPDATE SET 
        code_certification = EXCLUDED.code_certification,
        nom_certification = EXCLUDED.nom_certification;
    """

    for record in data:
        cur.execute(insert_query, record)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} certifications insérées ou mises à jour.")

dag = DAG(
     'dag_dim_certifications', 
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_certifications',
    python_callable=transform_certifications,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag,
)
start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting region extraction process..."),
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Region extraction process completed."),
    dag=dag
)


start_task>>extract_task >> transform_task >> load_task>>end_task
