import logging
import requests
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongo_collections():
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["PowerBi"]
    return client, db["offredemplois"], db["frontusers"], db["entreprises"]

def fetch_country_from_osm(city_name, cache):
    if city_name in cache:
        return cache[city_name]
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
            cache[city_name] = country
            return country
        else:
            return "Unknown"
    except Exception:
        return "Error"

def fetch_pays_data(cursor):
    cursor.execute("SELECT pays_id, nom_pays FROM public.dim_pays")
    return {country_name.lower(): pays_id for pays_id, country_name in cursor.fetchall()}

def generate_entreprise_code(counter):
    return f"entre{str(counter).zfill(4)}"

def get_next_entreprise_pk_and_code_counter(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(entreprise_id) FROM public.dim_entreprise;")
    max_pk = cur.fetchone()[0]
    
    cur.execute("SELECT MAX(CAST(SUBSTRING(code_entreprise FROM 6) AS INTEGER)) FROM public.dim_entreprise WHERE code_entreprise LIKE 'entre%';")
    max_code = cur.fetchone()[0]

    next_pk = (max_pk or 0) + 1
    next_code = (max_code or 0) + 1

    cur.close()
    return next_pk, next_code




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
    logger.info(f"{len(entreprises)} entreprises extraites de 'frontusers'.")
    return entreprises

def extract_from_entreprises(entreprises_col):
    entreprises = set()
    for doc in entreprises_col.find({}, {"nom": 1}):  
        nom = doc.get("nom", "").strip()
        if nom:
            entreprises.add((nom, "", None))  
    logger.info(f"{len(entreprises)} entreprises extraites de 'entreprises'.")
    return entreprises

def extract_all_entreprises(ti):
    client, offres_col, frontusers_col, entreprises_col = get_mongo_collections()
    villes_list = []
    for doc in offres_col.find({"isDeleted": False}, {"ville": 1}):
        ville = doc.get("ville", None)
        if ville and ville.strip() and ville not in villes_list:
            villes_list.append(ville.strip())
    entreprises = set()
    for sources in [
        extract_from_offredemplois(offres_col, villes_list),
        extract_from_frontusers(frontusers_col),
        extract_from_entreprises(entreprises_col)
    ]:
        entreprises.update(sources)
    merged_entreprises = {}
    for nom, ville, _ in entreprises:
        if nom not in merged_entreprises:
            merged_entreprises[nom] = {"ville": ""}
        if ville:
            merged_entreprises[nom]["ville"] = ville
    client.close()
    result = [(nom, data["ville"], None) for nom, data in merged_entreprises.items()] 
    ti.xcom_push(key='entreprises', value=result)
    logger.info(f"{len(result)} entreprises extraites au total.")


def transform_entreprises(**kwargs):
    entreprises_data = kwargs['ti'].xcom_pull(task_ids='extract_all_entreprises', key='entreprises')
    if not entreprises_data:
        logger.warning("Aucune entreprise à transformer.")
        return []

    transformed_data = []
    for entreprise in entreprises_data:
        nom, ville, _ = entreprise
      
        nom_transformed = nom.strip().title() 
        transformed_data.append((nom_transformed, ville, None))

    logger.info(f"{len(transformed_data)} entreprises transformées.")
    kwargs['ti'].xcom_push(key='transformed_entreprises', value=transformed_data)
    return transformed_data


def insert_entreprises(ti):
    entreprises = ti.xcom_pull(task_ids='transform_entreprises_task', key='transformed_entreprises')
    if not entreprises:
        return
    conn = get_postgres_connection()
    cur = conn.cursor()
    pays_mapping = fetch_pays_data(cur)
    counter_pk, counter_code = get_next_entreprise_pk_and_code_counter(conn)
    cur.execute("SELECT nom_entreprise FROM public.dim_entreprise")
    existing_names = {row[0].strip().lower() for row in cur.fetchall()}
    osm_cache = {}
    total = 0
    for nom, ville, _ in entreprises:  
        nom_clean = nom.strip()
        if not nom_clean or nom_clean.lower() in existing_names:
            continue
        pays_id = None
        if ville:
            country = fetch_country_from_osm(ville, osm_cache)
            pays_id = pays_mapping.get(country.lower())
        codeentreprise = generate_entreprise_code(counter_code)
        cur.execute(""" 
            INSERT INTO public.dim_entreprise (entreprise_id, code_entreprise, nom_entreprise, pays_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (nom_entreprise) DO UPDATE 
            SET code_entreprise = EXCLUDED.code_entreprise,
                pays_id = COALESCE(EXCLUDED.pays_id, dim_entreprise.pays_id);
        """, (
            counter_pk,
            codeentreprise,
            nom_clean,
            pays_id
        ))
        counter_pk += 1
        counter_code += 1
        total += 1
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{total} entreprises insérées ou mises à jour.")


dag = DAG(
    dag_id='dag_dim_entreprise',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting extraction process..."),
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_entreprises',
    python_callable=extract_all_entreprises,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_entreprises_task',
    python_callable=transform_entreprises,
    provide_context=True,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='load_entreprises',
    python_callable=insert_entreprises,
    provide_context=True,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Extraction process completed."),
    dag=dag,
)

start_task >> extract_task >> transform_task >> insert_task >> end_task
