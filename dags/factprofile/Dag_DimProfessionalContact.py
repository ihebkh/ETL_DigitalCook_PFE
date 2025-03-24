from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "frontusers"
        
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

def generate_contact_code():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT contact_code FROM dim_professional_contact ORDER BY contact_pk DESC LIMIT 1")
    last_code = cur.fetchone()

    if last_code and last_code[0].startswith("CONTACT"):
        last_number = int(last_code[0][7:])  
        new_number = last_number + 1
    else:
        new_number = 1 

    conn.close()
    return f"CONTACT{str(new_number).zfill(2)}" 

def extract_from_mongodb(**kwargs):
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})

        contacts = []
        existing_entries = set()

        for user in mongo_data:
            if "profile" in user and "proffessionalContacts" in user["profile"]:
                for contact in user["profile"]["proffessionalContacts"]:
                    contact_key = (contact.get("firstName"), contact.get("lastName"), contact.get("company"))
                    if contact_key not in existing_entries:
                        contacts.append({
                            "firstname": contact.get("firstName"),
                            "lastname": contact.get("lastName"),
                            "company": contact.get("company"),
                            "contact_code": None
                        })
                        existing_entries.add(contact_key)

        client.close()
        kwargs['ti'].xcom_push(key='mongo_data', value=contacts)
        logger.info(f"Extracted {len(contacts)} contacts from MongoDB.")
        return contacts
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def transform_data(**kwargs):
    mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
    transformed_contacts = []

    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("SELECT contact_code, firstname, lastname FROM dim_professional_contact")
    existing_contacts = {f"{row[1]}_{row[2]}": row[0] for row in cur.fetchall()} 

    conn.close()

    contact_counter = 1

    for record in mongo_data:
        contact_key = f"{record['firstname']}_{record['lastname']}"
        
        if contact_key in existing_contacts:
            record["contact_code"] = existing_contacts[contact_key]
        else:
            record["contact_code"] = f"CONTACT{str(contact_counter).zfill(2)}"
            contact_counter += 1
        
        transformed_contacts.append(record)

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_contacts)  # Push transformed data for loading
    return transformed_contacts

def load_into_postgres(**kwargs):
    try:
        transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

        if not transformed_data:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_professional_contact (contact_code, firstname, lastname, company)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (contact_code) DO UPDATE SET
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            company = EXCLUDED.company
        """

        for record in transformed_data:
            values = (
                record["contact_code"],
                record["firstname"],
                record["lastname"],
                record["company"]
            )
            logger.info(f"Inserting / Updating: {values}")
            cur.execute(insert_query, values)

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(transformed_data)} contact records inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimProfessionalContact',
    schedule_interval='*/2 * * * *',
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
    task_id='transform_data',
    python_callable=transform_data,
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
