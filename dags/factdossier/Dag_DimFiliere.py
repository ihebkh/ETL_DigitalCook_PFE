import logging
import re
from pymongo import MongoClient
from bson import ObjectId
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
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = client["PowerBi"]
    return client, mongo_db

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_filiere_code(index):
    return f"filiere{str(index).zfill(4)}"

def get_next_filiere_pk():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(filiere_pk) FROM public.dim_filiere;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def clean_price(prix):
    numeric_value = re.sub(r"[^\d.,]", "", prix or "")
    try:
        numeric_value = numeric_value.replace(",", ".")
        return float(numeric_value) if numeric_value else None
    except ValueError:
        return None

def extract_filieres(**kwargs):
    client, mongo_db = get_mongodb_connection()

    universities_coll = mongo_db["universities"]
    filieres_coll = mongo_db["filieres"]

    filieres_univ = universities_coll.find({}, {"_id": 0, "filiere": 1})
    filieres_data = filieres_coll.find({}, {"_id": 0, "nomfiliere": 1, "domaine": 1, "diplome": 1, "prix": 1, "prerequis": 1, "adresse": 1, "codepostal": 1})

    filieres = []

    for univ in filieres_univ:
        for f in univ.get("filiere", []):
            filieres.append(f)

    for record in filieres_data:
        filieres.append(record)

    client.close()

    filieres = convert_bson(filieres)
    kwargs['ti'].xcom_push(key='filieres_data', value=filieres)
    logger.info(f"{len(filieres)} filières extraites de MongoDB.")

def load_into_postgres(**kwargs):
    filieres = kwargs['ti'].xcom_pull(task_ids='extract_filieres', key='filieres_data')
    if not filieres:
        logger.info("Aucune filière à insérer.")
        return

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO dim_filiere (filiere_pk, filierecode, nomfiliere, domaine, diplome, prix, prerequis, adresse, codepostal)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (filiere_pk) DO UPDATE
    SET filierecode = EXCLUDED.filierecode,
        nomfiliere = EXCLUDED.nomfiliere,
        domaine = EXCLUDED.domaine,
        diplome = EXCLUDED.diplome,
        prix = EXCLUDED.prix,
        prerequis = EXCLUDED.prerequis,
        adresse = EXCLUDED.adresse,
        codepostal = EXCLUDED.codepostal;
    """

    counter = get_next_filiere_pk()

    for filiere in filieres:
        filiere_pk = counter
        code = generate_filiere_code(counter)
        nomfiliere = filiere.get("nomfiliere", "")
        domaine = filiere.get("domaine", "")
        diplome = filiere.get("diplome", "")
        prix = clean_price(filiere.get("prix", ""))
        prerequis = filiere.get("prerequis", "")
        adresse = filiere.get("adresse", "")
        codepostal = filiere.get("codepostal", "")

        cursor.execute(insert_query, (
            filiere_pk,
            code,
            nomfiliere,
            domaine,
            diplome,
            prix,
            prerequis,
            adresse,
            codepostal
        ))
        counter += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(filieres)} filières insérées/mises à jour.")

dag = DAG(
    dag_id='dag_dim_filiere',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_filieres',
    python_callable=extract_filieres,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_filieres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
