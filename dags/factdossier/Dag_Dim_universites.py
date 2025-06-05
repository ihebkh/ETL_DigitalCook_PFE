import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn



def get_mongodb_connection():
    MONGO_URI = Variable.get("MONGO_URI")
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "universities"
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    logger.info("MongoDB connection successful.")
    return client, mongo_db, collection


def generate_university_code(cursor):
    cursor.execute("SELECT MAX(CAST(SUBSTRING(code_universite FROM 5) AS INTEGER)) FROM public.dim_universite WHERE code_universite LIKE 'univ%'")
    max_code_number = cursor.fetchone()[0]
    if max_code_number is None:
        return "univ0001"
    new_number = max_code_number + 1
    return f"univ{str(new_number).zfill(4)}"

def fetch_pays_data(cursor):
    cursor.execute("SELECT pays_id, nom_pays_en FROM public.dim_pays")
    pays_data = cursor.fetchall()
    return {country_name.lower(): pays_id for pays_id, country_name in pays_data}

def extract_universities(**kwargs):
    client, _, collection = get_mongodb_connection()
    universities = []
    for record in collection.find({}, {"_id": 0, "nom": 1, "pays": 1}):
        nom = record.get("nom")
        pays = record.get("pays")
        if nom and pays:
            universities.append({"nom": nom, "pays": pays})
    client.close()
    kwargs['ti'].xcom_push(key='universities', value=universities)
    logger.info(f"{len(universities)} universities extracted.")

def load_universities_to_postgres(**kwargs):
    universities = kwargs['ti'].xcom_pull(task_ids='extract_universities', key='universities')
    if not universities:
        logger.info("No universities to insert.")
        return
    conn = get_postgres_connection()
    cursor = conn.cursor()
    pays_mapping = fetch_pays_data(cursor)
    insert_query = """
    INSERT INTO public.dim_universite (universite_id, code_universite, nom_universite, pays_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (nom_universite)
    DO UPDATE SET
        code_universite = EXCLUDED.code_universite,
        nom_universite = EXCLUDED.nom_universite,
        pays_id = EXCLUDED.pays_id;
    """
    pk_counter = 1
    inserted_count = 0
    for univ in universities:
        nom = univ["nom"]
        pays = univ["pays"]
        pays_id = pays_mapping.get(pays.lower())
        if not pays_id:
            logger.warning(f"Country '{pays}' not found in dim_pays. Skipping university '{nom}'.")
            continue
        code = generate_university_code(cursor)
        cursor.execute(insert_query, (pk_counter, code, nom, pays_id))
        inserted_count += 1
        pk_counter += 1
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{inserted_count} universities inserted or updated in PostgreSQL.")

dag = DAG(
    'dag_dim_universites',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_universities',
    python_callable=extract_universities,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_universities',
    python_callable=load_universities_to_postgres,
    provide_context=True,
    dag=dag,
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting university extraction process..."),
    dag=dag
)
end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("University extraction process completed."),
    dag=dag
)

start_task >> extract_task >> load_task >> end_task