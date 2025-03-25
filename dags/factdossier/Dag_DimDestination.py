import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["dossiers"]
    return collection

def extract_destinations(**kwargs):
    collection = get_mongodb_connection()
    results = collection.find({}, {"firstStep.destination": 1, "_id": 0})
    destinations = []

    for record in results:
        if "firstStep" in record and "destination" in record["firstStep"]:
            destinations.extend(record["firstStep"]["destination"])

    unique_destinations = list(set(destinations))
    kwargs['ti'].xcom_push(key='unique_destinations', value=unique_destinations)
    logger.info(f"{len(unique_destinations)} destinations extraites et nettoyées.")
    return unique_destinations

def generate_destination_codes(destinations):
    destinations_with_codes = []
    for i, destination in enumerate(destinations, start=1):
        destinations_with_codes.append({
            "destination_name": destination,
            "destination_code": f"dest{str(i).zfill(4)}"
        })
    return destinations_with_codes

def load_into_postgres(**kwargs):
    destinations = kwargs['ti'].xcom_pull(task_ids='extract_destinations', key='unique_destinations')
    if not destinations:
        logger.info("Aucune destination à charger.")
        return

    conn = get_postgresql_connection()
    cur = conn.cursor()

    cur.execute("SELECT destination_code FROM dim_destination")
    existing_codes = [row[0] for row in cur.fetchall() if row[0].startswith("dest")]

    def generate_new_code(index):
        return f"dest{str(index).zfill(4)}"

    next_code_number = 1
    used_codes = set(existing_codes)
    records = []

    for destination in destinations:
        while True:
            code_candidate = generate_new_code(next_code_number)
            next_code_number += 1
            if code_candidate not in used_codes:
                used_codes.add(code_candidate)
                break
        records.append({"destination_name": destination, "destination_code": code_candidate})

    insert_update_query = """
        INSERT INTO dim_destination (destination_name, destination_code)
        VALUES (%s, %s)
        ON CONFLICT (destination_name) DO UPDATE 
        SET destination_code = EXCLUDED.destination_code;
    """

    for record in records:
        cur.execute(insert_update_query, (record["destination_name"], record["destination_code"]))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(records)} enregistrements insérés/mis à jour dans PostgreSQL.")


dag = DAG(
    dag_id='dag_dim_destination',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_destinations',
    python_callable=extract_destinations,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_dim_destination',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
