import logging
import json
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
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

def get_next_partenaire_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(partenaire_pk) FROM public.dim_partenaire;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_partenaire_code(index):
    return f"part{str(index).zfill(4)}"

def extract_partenaires(**kwargs):
    client, mongo_db = get_mongodb_connection()
    collection = mongo_db["universities"]
    universities = collection.find({}, {"_id": 0, "partenairesProfessionnel": 1, "partenairesAcademique": 1})

    partenaires_professionnels = []
    partenaires_academiques = []

    for university in universities:
        partenaires_professionnels.extend(university.get("partenairesProfessionnel", []))
        partenaires_academiques.extend(university.get("partenairesAcademique", []))

    partenaires_professionnels = list(set(convert_bson(partenaires_professionnels)))
    partenaires_academiques = list(set(convert_bson(partenaires_academiques)))

    client.close()

    partenaires_professionnels = [f"professionnel {p}" for p in partenaires_professionnels]
    partenaires_academiques = [f"académique {p}" for p in partenaires_academiques]

    tous_les_partenaires = partenaires_professionnels + partenaires_academiques
    kwargs['ti'].xcom_push(key='partenaires_data', value=tous_les_partenaires)
    logger.info(f"{len(tous_les_partenaires)} partenaires extraits.")

def load_partenaires_postgres(**kwargs):
    partenaires = kwargs['ti'].xcom_pull(task_ids='extract_partenaires', key='partenaires_data')
    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO public.dim_partenaire (partenaire_pk, codepartenaire, nom_partenaire, typepartenaire)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (partenaire_pk)
    DO UPDATE SET
        codepartenaire = EXCLUDED.codepartenaire,
        nom_partenaire = EXCLUDED.nom_partenaire,
        typepartenaire = EXCLUDED.typepartenaire;
    """

    pk_counter = get_next_partenaire_pk()
    for partenaire in partenaires:
        code = generate_partenaire_code(pk_counter)
        partenaire_type = "professionnel" if "professionnel" in partenaire else "académique"
        partenaire_nom = partenaire.replace("professionnel ", "").replace("académique ", "")
        cursor.execute(insert_query, (
            pk_counter,
            code,
            partenaire_nom,
            partenaire_type
        ))
        pk_counter += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(partenaires)} partenaires insérés ou mis à jour.")

with DAG(
    dag_id='dag_dim_partenaire',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_partenaires',
        python_callable=extract_partenaires,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_partenaires_postgres',
        python_callable=load_partenaires_postgres,
        provide_context=True,
    )

    extract_task >> load_task
