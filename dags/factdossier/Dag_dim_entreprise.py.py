from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["frontusers"], db["entreprises"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def generate_entreprise_code(counter):
    return f"entre{counter:04d}"

def load_existing_entreprises():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT LOWER(nom), lieu_societe, codeentreprise FROM public.dim_entreprise;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {(nom.lower(), lieu): code for nom, lieu, code in rows if nom}

def extract_all_entreprises(**kwargs):
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
    kwargs['ti'].xcom_push(key='entreprises', value=list(entreprises))

def insert_entreprises(**kwargs):
    entreprises = kwargs['ti'].xcom_pull(task_ids='extract_all_entreprises', key='entreprises')
    existing = load_existing_entreprises()
    conn = get_postgres_connection()
    cur = conn.cursor()
    counter = len(existing) + 1
    total = 0
    for nom, lieu in entreprises:
        key = (nom.lower(), lieu)
        if key in existing:
            code = existing[key]
        else:
            code = generate_entreprise_code(counter)
            counter += 1
        cur.execute("""
            INSERT INTO public.dim_entreprise (codeentreprise, nom, lieu_societe)
            VALUES (%s, %s, %s)
            ON CONFLICT (codeentreprise) DO UPDATE
            SET nom = EXCLUDED.nom,
                lieu_societe = EXCLUDED.lieu_societe;
        """, (code, None if nom == "null" else nom, None if lieu == "null" else lieu))
        total += 1
    conn.commit()
    cur.close()
    conn.close()
    print(f"{total} entreprises insérées ou mises à jour dans dim_entreprise.")

dag = DAG(
    dag_id='dag_dim_entreprise_all_sources',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_all_entreprises',
    python_callable=extract_all_entreprises,
    provide_context=True,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_entreprises',
    python_callable=insert_entreprises,
    provide_context=True,
    dag=dag
)

extract_task >> insert_task
