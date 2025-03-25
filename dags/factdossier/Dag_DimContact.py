import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id="postgres")
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["universities"]
    return client, mongo_db, collection

def extract_contacts_from_mongodb(**kwargs):
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "contact.nom": 1, "contact.poste": 1, "contact.adresse": 1})
    contacts = []

    for university in universities:
        for contact in university.get("contact", []):
            contact_info = {
                "nom": contact.get("nom") or None,
                "poste": contact.get("poste") or None,
                "adresse": contact.get("adresse") or None
            }
            contacts.append(contact_info)

    client.close()
    kwargs['ti'].xcom_push(key='contacts', value=contacts)
    logger.info(f"{len(contacts)} contacts extraits depuis MongoDB.")
    return contacts

def generate_contact_code(index):
    return f"contact{str(index).zfill(4)}"

def insert_contacts_into_postgres(**kwargs):
    contacts = kwargs['ti'].xcom_pull(task_ids='extract_contacts', key='contacts')
    if not contacts:
        logger.info("Aucun contact à insérer.")
        return

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO dim_contact (contactcode, nom, poste, adresse)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (contactcode) DO UPDATE
    SET nom = EXCLUDED.nom,
        poste = EXCLUDED.poste,
        adresse = EXCLUDED.adresse;
    """

    for index, contact in enumerate(contacts, start=1):
        code = generate_contact_code(index)
        adresse = contact["adresse"]
        if adresse:
            adresse = adresse.strip('{}')
        if not contact["nom"] and not contact["poste"] and not adresse:
            logger.info(f"Skipping contact {code}, all fields are NULL.")
            continue
        cursor.execute(query, (code, contact["nom"], contact["poste"], adresse))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(contacts)} contacts insérés/mis à jour dans PostgreSQL.")

dag = DAG(
    dag_id='dag_dim_contact',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_contacts',
    python_callable=extract_contacts_from_mongodb,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_contacts',
    python_callable=insert_contacts_into_postgres,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
