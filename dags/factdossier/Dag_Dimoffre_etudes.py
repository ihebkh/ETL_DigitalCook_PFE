from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from pymongo import MongoClient
import psycopg2
from bson import ObjectId
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_collections():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["offredetudes"], db["universities"]

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def load_universite_id_to_name(universities_collection):
    cursor = universities_collection.find({}, {"_id": 1, "nom": 1})
    return {str(doc["_id"]): doc.get("nom", "").strip().lower() for doc in cursor}

def load_nom_to_codeuniversite():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT nom, codeuniversite FROM public.dim_universite;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {nom.strip().lower(): code for nom, code in rows if nom and code}

def generate_code_offre(counter: int):
    return f"ofed{counter:04d}"

def get_next_etude_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(etude_pk) FROM public.dim_offre_etude;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def load_existing_codeoffres():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT codeoffre FROM public.dim_offre_etude;")
    codes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return set(codes)

def insert_or_update_offres_batch(offres_data):
    existing_codes = load_existing_codeoffres()
    conn = get_postgres_connection()
    cur = conn.cursor()
    counter_code = len(existing_codes) + 1
    counter_pk = get_next_etude_pk()

    for titre, codeuniv, dispo, financement in offres_data:
        while True:
            code = generate_code_offre(counter_code)
            counter_code += 1
            if code not in existing_codes:
                existing_codes.add(code)
                break
        cur.execute("""
            INSERT INTO public.dim_offre_etude (
                etude_pk, codeoffre, titre, codeuniversite, disponibilite, financement
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (etude_pk) DO UPDATE
            SET codeoffre = EXCLUDED.codeoffre,
                titre = EXCLUDED.titre,
                codeuniversite = EXCLUDED.codeuniversite,
                disponibilite = EXCLUDED.disponibilite,
                financement = EXCLUDED.financement;
        """, (
            counter_pk,
            code,
            titre,
            codeuniv,
            dispo,
            financement
        ))
        counter_pk += 1

    conn.commit()
    cur.close()
    conn.close()

def extract_offres_etudes(**kwargs):
    offres_collection, universities_collection = get_mongodb_collections()
    id_to_name = load_universite_id_to_name(universities_collection)
    name_to_code = load_nom_to_codeuniversite()

    cursor = offres_collection.find({}, {
        "titre": 1,
        "university": 1,
        "disponibilite": 1,
        "criterias": 1
    })

    offres_to_insert = []
    for doc in cursor:
        titre = doc.get("titre", "—") 
        university_id = str(doc.get("university", "—"))
        university_name = id_to_name.get(university_id, "—").lower()
        university_code = name_to_code.get(university_name, "Non trouvé")
        disponibilite = doc.get("disponibilite", "—")
        financement = "—"
        for crit in doc.get("criterias", []):
            if crit.get("label", "").lower() in ["financement", "finance", "funding"]:
                financement = crit.get("value", "—")
                break
        offres_to_insert.append((titre, university_code, disponibilite, financement))

    insert_or_update_offres_batch(offres_to_insert)


dag = DAG(
    dag_id='dag_dim_offre_etudes',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

wait_dim_universite = ExternalTaskSensor(
    task_id='wait_for_dim_universite',
    external_dag_id='dag_dim_universite',
    external_task_id='load_dim_universite',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

extract_and_insert_task = PythonOperator(
    task_id="extract_et_insert_dim_offres",
    python_callable=extract_offres_etudes,
    dag=dag
)

wait_dim_universite >> extract_and_insert_task
