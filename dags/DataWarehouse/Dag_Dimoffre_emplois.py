import logging
import requests
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_country_from_osm(city_name):
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={city_name}"
        headers = {
            'User-Agent': 'YourAppName/1.0 (khmiriiheb3@gmail.com)',
            'Accept-Language': 'en'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return "Error"
        data = response.json()
        if data:
            display_name = data[0].get("display_name", "")
            country = display_name.split(",")[-1].strip()
            return country
        else:
            return "Unknown"
    except Exception as e:
        return "Error"

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    return conn

def get_secteur_map(cursor):
    try:
        cursor.execute("SELECT secteur_id, LOWER(nom_secteur) FROM public.dim_secteur;")
        return {label: pk for pk, label in cursor.fetchall()}
    except Exception as e:
        return {}

def get_entreprise_map(cursor):
    try:
        cursor.execute("SELECT entreprise_id, LOWER(nom_entreprise) FROM public.dim_entreprise;")
        return {label: pk for pk, label in cursor.fetchall()}
    except Exception as e:
        return {}

def get_country_map(cursor):
    try:
        cursor.execute("SELECT pays_id, LOWER(nom_pays_en) FROM public.dim_pays;")
        return {name: pk for pk, name in cursor.fetchall()}
    except Exception as e:
        return {}

def get_mongo_collections():
    MONGO_URI = Variable.get("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return client, db["offredemplois"], db["secteurdactivities"]

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def extract_offres_from_mongo():
    try:
        client, offres_col, _ = get_mongo_collections()
        cursor = offres_col.find({"isDeleted": False})
        offres = []
        for doc in cursor:
            offres.append({
                "_id": doc.get("_id"),
                "titre": doc.get("titre", "").strip(),
                "entreprise": doc.get("entreprise"),
                "secteur": doc.get("secteur"),
                "typeContrat": doc.get("typeContrat", "—"),
                "societe": doc.get("societe", "—"),
                "lieuSociete": doc.get("lieuSociete", "—"),
                "pays": doc.get("pays", "—"),
                "niveauDexperience": doc.get("niveauDexperience", "—")
            })
        client.close()
        cleaned = convert_bson(offres)
        return cleaned
    except Exception as e:
        return []

def get_last_offre_id(cursor):
    try:
        cursor.execute("SELECT COALESCE(MAX(offre_emploi_id), 0) FROM public.dim_offre_emploi;")
        return cursor.fetchone()[0]
    except Exception as e:
        return 0

def transform_offres(raw_offres, cursor):
    try:
        secteur_map = get_secteur_map(cursor)
        entreprise_map = get_entreprise_map(cursor)
        country_map = get_country_map(cursor)
        last_id = get_last_offre_id(cursor)
        seen_titles = set()
        counter = last_id + 1
        transformed = []
        for doc in raw_offres:
            titre = doc["titre"]
            if not titre or titre.lower() in seen_titles:
                continue
            seen_titles.add(titre.lower())
            offre_code = f"OFFR{str(counter).zfill(4)}"
            secteur_fk = entreprise_fk = None
            entreprise_fk = entreprise_map.get(doc["societe"].strip().lower())
            secteur_id = doc.get("secteur")
            if secteur_id and ObjectId.is_valid(secteur_id):
                client, _, secteurs_col = get_mongo_collections()
                secteur_doc = secteurs_col.find_one({"_id": ObjectId(secteur_id)})
                if secteur_doc:
                    label = secteur_doc.get("label", "").strip().lower()
                    secteur_fk = secteur_map.get(label)
            city_name = doc.get("lieuSociete", "").strip()
            country = fetch_country_from_osm(city_name) if city_name else "Unknown"
            country_id = country_map.get(country.lower(), None)
            niveau_experience = doc.get("niveauDexperience", "—")
            transformed.append({
                "offre_emploi_id": counter,
                "offre_code": offre_code,
                "titre": titre,
                "secteur_fk": secteur_fk,
                "entreprise_fk": entreprise_fk,
                "typeContrat": doc["typeContrat"],
                "pays_id": country_id,
                "niveauDexperience": niveau_experience
            })
            counter += 1
        return transformed
    except Exception as e:
        return []

def load_offres_to_postgres(transformed_offres, cursor, conn):
    try:
        for offre in transformed_offres:
            record = (
                offre.get('offre_emploi_id'),
                offre.get('offre_code'),
                offre.get('titre'),
                offre.get('secteur_fk', None),
                offre.get('entreprise_fk', None),
                offre.get('typeContrat'),
                offre.get('pays_id'),
                offre.get('niveauDexperience', "—")
            )
            if len(record) != 8:
                continue
            cursor.execute("""
                INSERT INTO public.dim_offre_emploi (
                    offre_emploi_id, code_offre_emploi, titre_offre_emploi, secteur_id, entreprise_id,
                    type_contrat_emploi, pays_emploi, niveau_experience
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (offre_emploi_id) DO UPDATE SET
                    code_offre_emploi = EXCLUDED.code_offre_emploi,
                    titre_offre_emploi = EXCLUDED.titre_offre_emploi,
                    secteur_id = EXCLUDED.secteur_id,
                    entreprise_id = EXCLUDED.entreprise_id,
                    type_contrat_emploi = EXCLUDED.type_contrat_emploi,
                    pays_emploi = EXCLUDED.pays_emploi,
                    niveau_experience = EXCLUDED.niveau_experience;
            """, record)
        conn.commit()
    except Exception as e:
        logger.error(f"Erreur insertion en base: {e}")
        raise

dag = DAG(
    dag_id='dag_dim_offre_emplois',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

def extract_task():
    raw_offres = extract_offres_from_mongo()
    return raw_offres

def transform_task(**kwargs):
    raw_offres = kwargs['ti'].xcom_pull(task_ids='extract_task')
    conn = get_postgres_connection()
    cursor = conn.cursor()
    try:
        transformed_offres = transform_offres(raw_offres, cursor)
        return transformed_offres
    finally:
        cursor.close()
        conn.close()

def load_task(**kwargs):
    transformed_offres = kwargs['ti'].xcom_pull(task_ids='transform_task')
    conn = get_postgres_connection()
    cursor = conn.cursor()
    try:
        load_offres_to_postgres(transformed_offres, cursor, conn)
    finally:
        cursor.close()
        conn.close()

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting formation extraction process..."),
    dag=dag
)

extract = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Formation extraction process completed."),
    dag=dag
)

start_task >> extract >> transform >> load >> end_task
