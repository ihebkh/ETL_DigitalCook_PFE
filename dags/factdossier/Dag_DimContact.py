import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = client["PowerBi"]
    return client, mongo_db

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_next_contact_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(contact_pk) FROM public.dim_contact;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def generate_contact_code(index):
    return f"contact{str(index).zfill(4)}"

def extract_contacts(**kwargs):
    client, mongo_db = get_mongodb_connection()
    universities_collection = mongo_db["universities"]
    frontusers_collection = mongo_db["frontusers"]
    centrefinancements_collection = mongo_db["centrefinancements"]

    contacts = []
    existing_entries = set()

    # Extracting contacts from universities
    universities = universities_collection.find({}, {"_id": 0, "contact.nom": 1, "contact.poste": 1, "contact.adresse": 1, "contact.company": 1, "contact.lastname": 1})
    for university in universities:
        if isinstance(university.get("contact"), list):  # Ensure "contact" is a list
            for contact in university["contact"]:
                if isinstance(contact, dict):  # Ensure contact is a dictionary
                    contact_info = {
                        "firstname": contact.get("nom"),
                        "lastname": contact.get("lastname"),
                        "poste": contact.get("poste"),
                        "company": contact.get("company"),
                    }
                    contacts.append(contact_info)

    # Extracting contacts from frontusers
    frontusers_data = frontusers_collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})
    for user in frontusers_data:
        if "profile" in user and "proffessionalContacts" in user["profile"]:
            proffessional_contact = user["profile"]["proffessionalContacts"]
            if isinstance(proffessional_contact, dict):  # Ensure it's a dictionary
                key = (proffessional_contact.get("firstName"), proffessional_contact.get("lastName"), proffessional_contact.get("company"))
                if key not in existing_entries:
                    contact_info = {
                        "firstname": proffessional_contact.get("firstName"),
                        "lastname": proffessional_contact.get("lastName"),
                        "company": proffessional_contact.get("company"),
                        "poste": proffessional_contact.get("poste"),
                    }
                    contacts.append(contact_info)
                    existing_entries.add(key)

    # Extracting contacts from centrefinancements
    centrefinancements_data = centrefinancements_collection.find({}, {"_id": 0, "contactPersonel.nom": 1, "contactPersonel.poste": 1})
    for record in centrefinancements_data:
        if isinstance(record.get("contactPersonel"), list):  # Ensure "contactPersonel" is a list
            for contact in record["contactPersonel"]:
                if isinstance(contact, dict):  # Ensure contact is a dictionary
                    contact_info = {
                        "firstname": contact.get("nom"),
                        "lastname": None,
                        "poste": contact.get("poste"),
                        "company": None,
                    }
                    contacts.append(contact_info)

    client.close()
    kwargs['ti'].xcom_push(key='contact_data', value=contacts)
    logger.info(f"{len(contacts)} contacts extraits.")

def transform_contacts(**kwargs):
    contacts = kwargs['ti'].xcom_pull(task_ids='extract_contacts', key='contact_data')
    
    transformed_contacts = []
    for contact in contacts:
        contact['firstname'] = contact.get('firstname', "Unknown") if contact.get('firstname') else "Unknown"
        contact['lastname'] = contact.get('lastname', "Unknown") if contact.get('lastname') else "Unknown"
        contact['poste'] = (contact.get('poste') or '').strip()
        contact['company'] = (contact.get('company') or '').strip()

        transformed_contacts.append(contact)
    
    kwargs['ti'].xcom_push(key='transformed_contact_data', value=transformed_contacts)
    logger.info(f"{len(transformed_contacts)} contacts transformés.")


def load_contacts_postgres(**kwargs):
    contacts = kwargs['ti'].xcom_pull(task_ids='transform_contacts', key='transformed_contact_data')
    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO public.dim_contact (contact_pk, contactcode, firstname, lastname, company, poste)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (firstname, lastname)
    DO UPDATE SET
        firstname = EXCLUDED.firstname,
        lastname = EXCLUDED.lastname,
        company = EXCLUDED.company,
        poste = EXCLUDED.poste,
        contactcode = EXCLUDED.contactcode;
    """

    counter_pk = get_next_contact_pk()
    for i, contact in enumerate(contacts, 0):
        code = generate_contact_code(counter_pk)
        cursor.execute(insert_query, (
            counter_pk,
            code,
            contact.get("firstname"),
            contact.get("lastname"),
            contact.get("company"),
            contact.get("poste"),
        ))
        counter_pk += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(contacts)} contacts insérés ou mis à jour.")

with DAG(
    dag_id='dag_dim_contact',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_contacts',
        python_callable=extract_contacts,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_contacts',
        python_callable=transform_contacts,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_contacts_postgres',
        python_callable=load_contacts_postgres,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
