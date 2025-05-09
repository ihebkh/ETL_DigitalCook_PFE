import logging
import requests
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

def get_next_entreprise_pk_and_code_counter(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(entreprise_id) FROM public.dim_entreprise;")
    max_pk = cur.fetchone()[0]
    cur.close()
    next_pk = (max_pk or 0) + 1
    return next_pk, next_pk

def fetch_country_from_osm(city_name):
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={city_name}"
        headers = {
            'User-Agent': 'YourAppName/1.0 (khmiriiheb3@gmail.com)',
            'Accept-Language': 'en'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Error fetching data from Nominatim for {city_name}: {response.status_code}")
            return "Error"
        data = response.json()
        if data:
            display_name = data[0].get("display_name", "")
            country = display_name.split(",")[-1].strip()
            logger.info(f"Extracted country for {city_name}: {country}")
            return country
        else:
            logger.warning(f"No data found for city: {city_name}")
            return "Unknown"
    except Exception as e:
        logger.error(f"Error fetching data from Nominatim for {city_name}: {e}")
        return "Error"

def fetch_pays_data(cursor):
    cursor.execute("SELECT pays_id, nom_pays_en FROM public.dim_pays")
    pays_data = cursor.fetchall()
    return {country_name.lower(): pays_id for pays_id, country_name in pays_data}

def extract_from_offredemplois(offres_col, villes_list=None):
    if villes_list is None:
        villes_list = []
    entreprises = set()
    for doc in offres_col.find({"isDeleted": False}, {"societe": 1, "ville": 1}):
        nom = doc.get("societe", "").strip()
        ville_doc = doc.get("ville", "").strip()
        if nom and (ville_doc in villes_list or not villes_list):
            entreprises.add((nom, ville_doc, None))
    logger.info(f"{len(entreprises)} entreprises extraites de 'offredemplois'.")
    return entreprises

def extract_from_frontusers(frontusers_col):
    entreprises = set()
    for doc in frontusers_col.find({}, {
        "profile.experiences.entreprise": 1,
        "profile.experiences.pays": 1,
        "simpleProfile.experiences.entreprise": 1,
        "simpleProfile.experiences.pays": 1
    }):
        for profile_key in ["profile", "simpleProfile"]:
            profile = doc.get(profile_key, {})
            for exp in profile.get("experiences", []):
                if isinstance(exp, dict):
                    nom = exp.get("entreprise", "")
                    pays = exp.get("pays", "")
                    if isinstance(pays, dict):
                        pays = pays.get("value", "")
                    elif not isinstance(pays, str):
                        pays = ""
                    nom = nom.strip() if isinstance(nom, str) else ""
                    pays = pays.strip()
                    if nom:
                        entreprises.add((nom, "", None))
    logger.info(f"{len(entreprises)} entreprises extraites de 'frontusers' avec pays.")
    return entreprises

def extract_from_entreprises(entreprises_col):
    entreprises = set()
    for doc in entreprises_col.find({}, {"nom": 1, "ville": 1, "nombreEmployes": 1}):
        nom = doc.get("nom", None).strip()
        ville = doc.get("ville", "").strip()
        nombre_employes = doc.get("nombreEmployes", None)
        if nom:
            entreprises.add((nom, ville, nombre_employes)) 
    logger.info(f"{len(entreprises)} entreprises extraites de 'entreprises' avec nombre d'employés.")
    return entreprises

def extract_all_entreprises(ti):
    client, offres_col, frontusers_col, entreprises_col = get_mongo_collections()
    entreprises = set()
    villes_list = []
    for doc in offres_col.find({"isDeleted": False}, {"ville": 1}):
        ville = doc.get("ville", None)
        if ville and ville.strip() and ville not in villes_list:
            villes_list.append(ville.strip())
    entreprises.update(extract_from_offredemplois(offres_col, villes_list=villes_list))
    entreprises.update(extract_from_frontusers(frontusers_col))
    entreprises.update(extract_from_entreprises(entreprises_col))
    client.close()
    entreprises_list = list(entreprises)
    ti.xcom_push(key='entreprises', value=entreprises_list)
    logger.info(f"{len(entreprises_list)} entreprises extraites au total.")

def insert_entreprises(ti):
    entreprises = ti.xcom_pull(task_ids='extract_all_entreprises', key='entreprises')
    if not entreprises:
        logger.info("Aucune entreprise à insérer.")
        return
    conn = get_postgres_connection()
    cur = conn.cursor()
    pays_mapping = fetch_pays_data(cur)
    counter_pk, counter_code = get_next_entreprise_pk_and_code_counter(conn)
    total = 0
    for nom, ville, nombre_employes in entreprises:
        nom_clean = nom.strip() if nom else None
        if not nom_clean:
            continue
        pays_id = None
        if ville:
            country = fetch_country_from_osm(ville)
            pays_id = pays_mapping.get(country.lower())
        codeentreprise = generate_entreprise_code(counter_code)
        cur.execute("""
            INSERT INTO public.dim_entreprise (entreprise_id, code_entreprise, nom_entreprise, nombre_employes, pays_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (nom_entreprise) DO UPDATE
            SET 
                code_entreprise = EXCLUDED.code_entreprise,
                nom_entreprise = EXCLUDED.nom_entreprise,
                nombre_employes = EXCLUDED.nombre_employes,
                pays_id = EXCLUDED.pays_id;
        """, (
            counter_pk,
            codeentreprise,
            nom_clean,
            nombre_employes,
            pays_id
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

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting extraction process..."),
    dag=dag,
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

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Extraction process completed."),
    dag=dag,
)

start_task >> extract_task >> insert_task >> end_task