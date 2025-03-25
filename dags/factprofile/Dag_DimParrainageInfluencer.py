import logging
import psycopg2
from pymongo import MongoClient
import tempfile
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "parrainageinfluencers"

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

def extract_full_names_from_mongodb(**kwargs):
    """ Extract data from MongoDB and save it to a temporary file. """
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = collection.find({}, {"_id": 0, "fullName": 1})

        full_names = set()

        for user in mongo_data:
            if isinstance(user, dict):
                full_name = user.get("fullName", "").strip()
                if full_name:
                    full_names.add(full_name)

        client.close()

        logger.info(f"Full names extracted: {len(full_names)}")

        # Save to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
            temp_file_path = temp_file.name
            json.dump([{"fullName": name} for name in full_names], temp_file, ensure_ascii=False, indent=4)
        
        kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
        return [{"fullName": name} for name in full_names]
    except Exception as e:
        logger.error(f"Error extracting full names from MongoDB: {e}")
        raise

def load_full_names_into_postgres(**kwargs):
    """ Load the extracted full names into PostgreSQL. """
    try:
        temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_full_names_from_mongodb', key='temp_file_path')

        with open(temp_file_path, 'r', encoding='utf-8') as file:
            full_names = json.load(file)

        if not full_names:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_parinnage_influencer (fullName)
        VALUES (%s)
        ON CONFLICT (fullName) DO NOTHING;
        """
        update_query = """
        UPDATE dim_parinnage_influencer
        SET fullName = %s
        WHERE fullName = %s;
        """

        cur.execute("SELECT fullName FROM dim_parinnage_influencer")
        existing_full_names = {row[0] for row in cur.fetchall()}

        for record in full_names:
            if record["fullName"] in existing_full_names:
                logger.info(f"Updating full name: {record['fullName']}")
                cur.execute(update_query, (record["fullName"], record["fullName"]))
            else:
                logger.info(f"Inserting full name: {record['fullName']}")
                cur.execute(insert_query, (record["fullName"],))

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"{len(full_names)} full names inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading full names into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimParrainageInfluencer',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Extract task
extract_task = PythonOperator(
    task_id='extract_full_names_from_mongodb',
    python_callable=extract_full_names_from_mongodb,
    provide_context=True,
    dag=dag,
)

# Load task
load_task = PythonOperator(
    task_id='load_full_names_into_postgres',
    python_callable=load_full_names_into_postgres,
    provide_context=True,
    dag=dag,
)

# Set dependencies
extract_task >> load_task