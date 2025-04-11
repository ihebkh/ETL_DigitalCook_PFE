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

    universities = universities_collection.find({}, {"_id": 0, "contact.nom": 1, "contact.poste": 1, "contact.adresse": 1, "contact.company": 1, "contact.lastname": 1})
    for university in universities:
        for contact in university.get("contact", []):
            contact_info = {
                "firstname": contact.get("nom"),
                "lastname": contact.get("lastname"),
                "poste": contact.get("poste"),
                "adresse": contact.get("adresse"),
                "company": contact.get("company"),
                "typecontact": "Université"
            }
            contacts.append(contact_info)

    frontusers_data = frontusers_collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})
    for user in frontusers_data:
        if "profile" in user and "proffessionalContacts" in user["profile"]:
            for contact in user["profile"]["proffessionalContacts"]:
                key = (contact.get("firstName"), contact.get("lastName"), contact.get("company"))
                if key not in existing_entries:
                    contact_info = {
                        "firstname": contact.get("firstName"),
                        "lastname": contact.get("lastName"),
                        "company": contact.get("company"),
                        "poste": contact.get("poste"),
                        "adresse": contact.get("adresse"),
                        "typecontact": "Professionnel"
                    }
                    contacts.append(contact_info)
                    existing_entries.add(key)

    centrefinancements_data = centrefinancements_collection.find({}, {"_id": 0, "contactPersonel.nom": 1, "contactPersonel.poste": 1})
    for record in centrefinancements_data:
        for contact in record.get("contactPersonel", []):
            contact_info = {
                "firstname": contact.get("nom"),
                "lastname": None,
                "poste": contact.get("poste"),
                "adresse": None,
                "company": None,
                "typecontact": "Personnel"
            }
            contacts.append(contact_info)

    client.close()
    kwargs['ti'].xcom_push(key='contact_data', value=contacts)
    logger.info(f"{len(contacts)} contacts extraits.")

def load_contacts_postgres(**kwargs):
    contacts = kwargs['ti'].xcom_pull(task_ids='extract_contacts', key='contact_data')
    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO public.dim_contact (contact_pk, contactcode, firstname, lastname, company, typecontact, poste, adresse)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (contact_pk)
    DO UPDATE SET
        firstname = EXCLUDED.firstname,
        lastname = EXCLUDED.lastname,
        company = EXCLUDED.company,
        typecontact = EXCLUDED.typecontact,
        poste = EXCLUDED.poste,
        adresse = EXCLUDED.adresse,
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
            contact.get("typecontact"),
            contact.get("poste"),
            contact.get("adresse")
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

    load_task = PythonOperator(
        task_id='load_contacts_postgres',
        python_callable=load_contacts_postgres,
        provide_context=True,
    )

    extract_task >> load_task
