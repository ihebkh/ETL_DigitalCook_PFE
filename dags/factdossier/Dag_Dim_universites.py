import logging
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "formations"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def generate_university_code(cursor):
    cursor.execute("SELECT MAX(CAST(SUBSTRING(code_univ FROM 5) AS INTEGER)) FROM public.dim_universite WHERE code_univ LIKE 'univ%'")
    max_code_number = cursor.fetchone()[0]

    if max_code_number is None:
        return "univ0001"

    new_number = max_code_number + 1
    return f"univ{str(new_number).zfill(4)}"

def extract_pays_and_universities(**kwargs):
    client, _, collection = get_mongodb_connection()

    universities = []
    countries = set()

    universities_data = collection.find({}, {"_id": 0, "nom": 1, "pays": 1})
    for record in universities_data:
        if "nom" in record and "pays" in record:
            universities.append(record["nom"])
            countries.add(record["pays"])

    client.close()

    kwargs['ti'].xcom_push(key='universities', value=universities)
    kwargs['ti'].xcom_push(key='countries', value=list(countries))

    logger.info(f"{len(universities)} universities extracted.")
    logger.info(f"{len(countries)} countries extracted.")

def load_universities_to_postgres(**kwargs):
    universities = kwargs['ti'].xcom_pull(task_ids='extract_universities', key='universities')
    countries = kwargs['ti'].xcom_pull(task_ids='extract_universities', key='countries')

    if universities is None or countries is None:
        logger.error("XCom pull failed: 'universities' or 'countries' is None. Check upstream task and XCom keys.")
        raise ValueError("XCom pull failed: 'universities' or 'countries' is None.")

    logger.info(f"Received {len(universities)} universities from XCom.")
    logger.info(f"Received {len(countries)} countries from XCom.")

    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO public.dim_universite (universite_id, code_universite, nom_universite, pays_universite)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (nom_universite)
    DO UPDATE SET
        code_universite = EXCLUDED.code_universite,
        nom_universite = EXCLUDED.nom_universite,
        pays_universite = EXCLUDED.pays_universite;
    """

    pk_counter = 1  # Start university_id at 1
    inserted_count = 0

    for university in universities:
        code = generate_university_code(cursor)
        for country in countries:
            cursor.execute(insert_query, (pk_counter, code, university, country))
            inserted_count += 1
            pk_counter += 1  # Increment university_id for each new university

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"{inserted_count} universities inserted or updated in PostgreSQL.")

dag = DAG(
    'dag_dim_universites',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_universities',
    python_callable=extract_pays_and_universities,
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
        python_callable=lambda: logger.info("Starting recruitment extraction process..."),
 )
end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Recruitment extraction process completed."),
)


start_task>>extract_task >> load_task>>end_task
