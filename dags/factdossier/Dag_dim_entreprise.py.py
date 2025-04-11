import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["frontusers"], db["entreprises"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def generate_entreprise_code(counter):
    return f"entre{str(counter).zfill(4)}"

def get_next_entreprise_pk_and_code_counter():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(entreprise_pk) FROM public.dim_entreprise;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    next_pk = (max_pk or 0) + 1
    return next_pk, next_pk

def extract_all_entreprises(ti):
    client, offres_col, frontusers_col, entreprises_col = get_mongo_collections()
    entreprises = set()

    for doc in offres_col.find({"isDeleted": False}, {"societe": 1, "lieuSociete": 1}):
        nom = doc.get("societe", "").strip() or "null"
        lieu = doc.get("lieuSociete", "").strip() or "null"
        if nom != "null" or lieu != "null":
            entreprises.add((nom, lieu))

    for doc in frontusers_col.find({}, {"profile.experiences.entreprise": 1, "simpleProfile.experiences.entreprise": 1}):
        for profile_key in ["profile", "simpleProfile"]:
            profile = doc.get(profile_key, {})
            for exp in profile.get("experiences", []):
                if isinstance(exp, dict):
                    nom = exp.get("entreprise", "").strip() or "null"
                    if nom != "null":
                        entreprises.add((nom, "null"))

    for doc in entreprises_col.find({}, {"nom": 1, "ville": 1}):
        nom = doc.get("nom", "").strip() or "null"
        ville = doc.get("ville", "").strip() or "null"
        if nom != "null" or ville != "null":
            entreprises.add((nom, ville))

    client.close()
    ti.xcom_push(key='entreprises', value=list(entreprises))

def insert_entreprises(ti):
    entreprises = ti.xcom_pull(task_ids='extract_all_entreprises', key='entreprises')
    conn = get_postgres_connection()
    cur = conn.cursor()

    counter_pk, counter_code = get_next_entreprise_pk_and_code_counter()
    total = 0

    for nom, lieu in entreprises:
        nom_clean = None if nom == "null" else nom
        lieu_clean = None if lieu == "null" else lieu

        entreprise_pk = counter_pk
        codeentreprise = generate_entreprise_code(counter_code)

        cur.execute("""
            INSERT INTO public.dim_entreprise (entreprise_pk, codeentreprise, nom, lieu_societe)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (entreprise_pk) DO UPDATE
            SET nom = EXCLUDED.nom,
                lieu_societe = EXCLUDED.lieu_societe,
                codeentreprise = EXCLUDED.codeentreprise;
        """, (
            entreprise_pk,
            codeentreprise,
            nom_clean,
            lieu_clean
        ))

        counter_pk += 1
        counter_code += 1
        total += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{total} entreprises insérées ou mises à jour dans dim_entreprise.")

dag = DAG(
    dag_id='dag_dim_entreprise',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_all_entreprises',
    python_callable=extract_all_entreprises,
    provide_context=True,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_entreprises',
    python_callable=insert_entreprises,
    provide_context=True,
    dag=dag,
)

extract_task >> insert_task
