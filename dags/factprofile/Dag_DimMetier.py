import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    return conn



def get_mongodb_connection():
    try:
        mongo_uri = Variable.get("MONGO_URI")
        client = MongoClient(mongo_uri)
        db = client["PowerBi"]
        collection = db["secteurdactivities"]
        logger.info("MongoDB connection successful.")
        return client, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def get_max_metier_pk(conn):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(metier_id), 0) FROM Dim_Metier")
    max_pk = cur.fetchone()[0]
    cur.close()
    return max_pk

def get_existing_rome_codes(conn):
    cur = conn.cursor()
    cur.execute("SELECT code_metier FROM Dim_Metier")
    rome_codes = {row[0] for row in cur.fetchall()}
    cur.close()
    return rome_codes

def extract_jobs_from_mongodb(**kwargs):
    try:
        client, collection = get_mongodb_connection()
        cursor = collection.find({}, {"_id": 0, "jobs": 1})

        jobs = []
        seen_labels = set()
        for document in cursor:
            if "jobs" in document:
                for job in document["jobs"]:
                    if job.get("romeCode"):
                        job_info = {
                            "label": job.get("label"),
                            "romeCode": job.get("romeCode")
                        }
                        if job_info["romeCode"] not in seen_labels:
                            jobs.append(job_info)
                            seen_labels.add(job_info["romeCode"])

        client.close()
        kwargs['ti'].xcom_push(key='extracted_jobs', value=jobs)
        logger.info(f"{len(jobs)} jobs extracted from MongoDB.")
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def generate_langue_code(index):
    return f"LANG{str(index).zfill(3)}"

def transform_jobs_data(**kwargs):
    try:
        jobs = kwargs['ti'].xcom_pull(task_ids='extract_jobs_from_mongodb', key='extracted_jobs')
        if not jobs:
            logger.info("No jobs to transform.")
            return

        conn = get_postgres_connection()
        max_pk = get_max_metier_pk(conn)
        existing_rome_codes = get_existing_rome_codes(conn)

        transformed = []
        compteur = max_pk

        for job in jobs:
            if job["romeCode"] not in existing_rome_codes:
                compteur += 1
                transformed.append({
                    "metier_pk": compteur,
                    "romeCode": job["romeCode"],
                    "label": job["label"]
                })
                existing_rome_codes.add(job["romeCode"])

        kwargs['ti'].xcom_push(key='transformed_jobs', value=transformed)
        logger.info(f"{len(transformed)} new jobs prepared for PostgreSQL.")
    except Exception as e:
        logger.error(f"Error transforming jobs: {e}")
        raise

def load_jobs_into_postgres(**kwargs):
    try:
        jobs_data = kwargs['ti'].xcom_pull(task_ids='transform_jobs_data', key='transformed_jobs')
        if not jobs_data:
            logger.info("No jobs to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO Dim_Metier (metier_id, code_metier, nom_metier)
        VALUES (%s, %s, %s)
        ON CONFLICT (metier_id) DO UPDATE SET
            code_metier = EXCLUDED.code_metier,
            nom_metier = EXCLUDED.nom_metier;
        """

        for job in jobs_data:
            cur.execute(insert_query, (
                job["metier_pk"], job["romeCode"], job["label"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(jobs_data)} jobs inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading jobs into PostgreSQL: {e}")
        raise

dag = DAG(
    'dag_dim_metier',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting region extraction process..."),
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_jobs_from_mongodb',
    python_callable=extract_jobs_from_mongodb,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_jobs_data',
    python_callable=transform_jobs_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_jobs_into_postgres',
    python_callable=load_jobs_into_postgres,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Region extraction process completed."),
    dag=dag
)

start_task >> extract_task >> transform_task >> load_task >> end_task
