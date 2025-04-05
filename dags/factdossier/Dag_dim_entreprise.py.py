from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Connexions MongoDB
def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["frontusers"]

# Générer code entreprise unique
def generate_entreprise_code(counter):
    return f"entre{counter:04d}"

# Charger entreprises existantes
def load_existing_entreprises():
    conn = PostgresHook(postgres_conn_id="postgres").get_conn()
    cur = conn.cursor()
    cur.execute("SELECT LOWER(nom), lieu_societe, codeentreprise FROM public.dim_entreprise;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {(nom.lower(), lieu): code for nom, lieu, code in rows if nom}

# Extraction MongoDB
def extract_entreprises(**kwargs):
    client, offres_col, frontusers_col = get_mongo_collections()
    entreprises = set()

    print("📄 Offres d'emploi :")
    for doc in offres_col.find({"isDeleted": False}, {"societe": 1, "lieuSociete": 1}):
        nom = doc.get("societe")
        lieu = doc.get("lieuSociete")
        nom_aff = nom.strip() if nom else "null"
        lieu_aff = lieu.strip() if lieu else "null"
        if nom_aff != "null" or lieu_aff != "null":
            print(f"- {nom_aff} , {lieu_aff}")
            entreprises.add((nom_aff, lieu_aff))

    print("\n📄 Frontusers :")
    for doc in frontusers_col.find({}, {"profile.experiences.entreprise": 1, "simpleProfile.experiences.entreprise": 1}):
        for profile_key in ["profile", "simpleProfile"]:
            profile = doc.get(profile_key, {})
            for exp in profile.get("experiences", []):
                if isinstance(exp, dict):
                    nom = exp.get("entreprise")
                    nom_aff = nom.strip() if nom else "null"
                    if nom_aff != "null":
                        print(f"- {nom_aff} , null")
                        entreprises.add((nom_aff, "null"))

    client.close()
    kwargs['ti'].xcom_push(key='entreprises', value=list(entreprises))

# Insertion PostgreSQL
def insert_entreprises(**kwargs):
    entreprises = kwargs['ti'].xcom_pull(task_ids='extract_entreprises', key='entreprises')
    existing = load_existing_entreprises()

    conn = PostgresHook(postgres_conn_id="postgres").get_conn()
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
        """, (code, nom if nom != "null" else None, lieu if lieu != "null" else None))
        total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ {total} entreprises insérées ou mises à jour dans dim_entreprise.")

dag = DAG(
    dag_id='dag_dim_entreprise',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_entreprises',
    python_callable=extract_entreprises,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='insert_entreprises',
    python_callable=insert_entreprises,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
