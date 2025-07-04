import logging
import requests
from time import sleep
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = Variable.get("MONGO_URI")
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return client, db["frontusers"]

def get_postgres_connection():
    return PostgresHook(postgres_conn_id='postgres').get_conn()

def fetch_country_from_osm(city_name):
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={city_name}"
        headers = {
            'User-Agent': 'Airflow-DAG/1.0 (email@example.com)',
            'Accept-Language': 'en'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"Erreur OSM pour {city_name}: {response.status_code}")
            return "Unknown"
        data = response.json()
        if data:
            display_name = data[0].get("display_name", "")
            return display_name.split(",")[-1].strip()
        return "Unknown"
    except Exception as e:
        logger.error(f"Erreur OSM {city_name}: {e}")
        return "Unknown"

def generate_location_code(cursor):
    cursor.execute("""
        SELECT MAX(CAST(SUBSTRING(code_ville FROM 4) AS INTEGER))
        FROM dim_ville WHERE code_ville LIKE 'LOC%'
    """)
    max_code = cursor.fetchone()[0] or 0
    return f"LOC{str(max_code + 1).zfill(3)}"

def get_existing_villes(cursor):
    cursor.execute("SELECT nom_ville FROM dim_ville")
    return {row[0] for row in cursor.fetchall()}

def fetch_pays_data(cursor):
    cursor.execute("SELECT pays_id, nom_pays FROM dim_pays")
    return {name.lower(): pid for pid, name in cursor.fetchall()}


def extract_villes_from_mongo(**kwargs):
    client, collection = get_mongodb_connection()
    villes = set()
    for doc in collection.find({}, {"_id": 0, "profile.preferedJobLocations": 1, "simpleProfile.preferedJobLocations": 1}):
        for section in ["profile", "simpleProfile"]:
            locations = doc.get(section, {}).get("preferedJobLocations", [])
            for loc in locations:
                if isinstance(loc, dict) and "ville" in loc:
                    villes.add(loc["ville"])
    client.close()
    villes_list = list(villes)
    logger.info(f"{len(villes_list)} villes extraites de MongoDB.")
    kwargs['ti'].xcom_push(key="raw_villes", value=villes_list)


def transform_villes_with_country_lookup(**kwargs):
    raw_villes = kwargs['ti'].xcom_pull(task_ids='extract_villes_from_mongo', key='raw_villes')
    if not raw_villes:
        logger.warning("Aucune ville reçue depuis XCom. Arrêt de la tâche transform.")
        return

    mapping = {}
    for ville in raw_villes:
        country = fetch_country_from_osm(ville)
        mapping[ville] = country
        logger.info(f"{ville} → {country}")
        sleep(1) 

    logger.info(f"{len(mapping)} couples ville/pays récupérés.")
    kwargs['ti'].xcom_push(key="ville_country_mapping", value=mapping)


def load_villes_and_countries_postgres(**kwargs):
    mapping = kwargs['ti'].xcom_pull(task_ids='transform_villes_with_country_lookup', key='ville_country_mapping')
    if not mapping:
        logger.warning("Aucun mapping ville-pays trouvé. Rien à charger.")
        return

    conn = get_postgres_connection()
    with conn:
        with conn.cursor() as cursor:
            pays_mapping = fetch_pays_data(cursor)
            existing_villes = get_existing_villes(cursor)

            insert_query = """
            INSERT INTO dim_ville (ville_id, code_ville, nom_ville, pays_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (nom_ville)
            DO UPDATE SET
                code_ville = EXCLUDED.code_ville,
                pays_id = EXCLUDED.pays_id;
            """

            pk = 1
            inserted = 0
            for ville, pays in mapping.items():
                if ville in existing_villes:
                    continue
                pays_id = pays_mapping.get(pays.lower())
                if not pays_id:
                    logger.warning(f"Pays inconnu pour {pays}, ville ignorée : {ville}")
                    continue
                code = generate_location_code(cursor)
                cursor.execute(insert_query, (pk, code, ville, pays_id))
                pk += 1
                inserted += 1

    logger.info(f"{inserted} villes insérées ou mises à jour dans PostgreSQL.")

dag = DAG(
    dag_id='dag_dim_villes',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info(" Début du traitement des villes."),
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_villes',
    python_callable=extract_villes_from_mongo,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_villes',
    python_callable=transform_villes_with_country_lookup,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_villes',
    python_callable=load_villes_and_countries_postgres,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info(" Fin du traitement des villes."),
    dag=dag
)

start_task >> extract_task >> transform_task >> load_task >> end_task
