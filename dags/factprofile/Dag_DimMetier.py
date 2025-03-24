import json
import logging
import psycopg2
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "secteurdactivities"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def extract_jobs_from_mongodb(**kwargs):
    """ Extract job data from MongoDB and save to XCom. """
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = collection.find({}, {"_id": 0, "jobs": 1})

        jobs = []

        for document in mongo_data:
            if "jobs" in document:
                for job in document["jobs"]:
                    job_info = {
                        "label": job.get("label"),
                        "romeCode": job.get("romeCode"),
                        "mainName": job.get("mainName"),
                        "subDomain": job.get("subDomain")
                    }
                    logger.info(f"Extracted job: {job_info}")
                    jobs.append(job_info)

        client.close()
        kwargs['ti'].xcom_push(key='jobs_data', value=jobs)
        return jobs
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def load_jobs_into_postgres(**kwargs):
    """ Load the extracted jobs data into PostgreSQL. """
    try:
        jobs_data = kwargs['ti'].xcom_pull(task_ids='extract_jobs_from_mongodb', key='jobs_data')

        if not jobs_data:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        for job in jobs_data:
            try:
                cur.execute("""
                    INSERT INTO Dim_Metier (romeCode, label_jobs, mainname_jobs, subdomain_jobs)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (romeCode) DO UPDATE SET
                        label_jobs = EXCLUDED.label_jobs,
                        mainname_jobs = EXCLUDED.mainname_jobs,
                        subdomain_jobs = EXCLUDED.subdomain_jobs;
                """, (job["romeCode"], job["label"], job["mainName"], job["subDomain"]))
            except Exception as e:
                logger.error(f"Error inserting job {job['romeCode']}: {e}")

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(jobs_data)} jobs inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading jobs into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_Metier',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_jobs_from_mongodb',
    python_callable=extract_jobs_from_mongodb,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_jobs_into_postgres',
    python_callable=load_jobs_into_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task
