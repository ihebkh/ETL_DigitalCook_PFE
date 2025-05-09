import logging
import requests
from time import sleep
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION_1 = "frontusers"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection_1 = mongo_db[MONGO_COLLECTION_1]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection_1
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def generate_location_code(cursor, existing_codes):
    if not existing_codes:
        return "LOC001"
    cursor.execute("SELECT MAX(CAST(SUBSTRING(code_ville FROM 4) AS INTEGER)) FROM public.dim_ville WHERE code_ville LIKE 'LOC%'")
    max_code_number = cursor.fetchone()[0]
    if max_code_number is None:
        return "LOC001"
    new_number = max_code_number + 1
    return f"LOC{str(new_number).zfill(3)}"

def get_existing_villes(cursor):
    cursor.execute("SELECT nom_ville FROM public.dim_ville")
    existing_entries = {row[0] for row in cursor.fetchall()}
    return existing_entries

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

def extract_villes_from_frontusers(**kwargs):
    client, _, collection_1 = get_mongodb_connection()
    villes = set()
    frontusers_data = collection_1.find({}, {"_id": 0, "profile.preferedJobLocations": 1, "simpleProfile.preferedJobLocations": 1})
    for record in frontusers_data:
        for profile_key in ["profile", "simpleProfile"]:
            if profile_key in record and "preferedJobLocations" in record[profile_key]:
                for location in record[profile_key]["preferedJobLocations"]:
                    if isinstance(location, dict) and "ville" in location:
                        villes.add(location["ville"])
    villes = list(villes)
    client.close()
    city_country_mapping = {}
    for ville in villes:
        country = fetch_country_from_osm(ville)
        city_country_mapping[ville] = country
        logger.info(f"City: {ville}, Country: {country}")
        print(f"City: {ville}, Country: {country}")
        sleep(1)
    kwargs['ti'].xcom_push(key='city_country_mapping', value=city_country_mapping)
    logger.info(f"{len(villes)} villes extraites depuis frontusers, avec pays associés.")

def fetch_pays_data(cursor):
    cursor.execute("SELECT pays_id, nom_pays_en FROM public.dim_pays")
    pays_data = cursor.fetchall()
    return {country_name.lower(): pays_id for pays_id, country_name in pays_data}

def load_villes_and_countries_postgres(**kwargs):
    city_country_mapping = kwargs['ti'].xcom_pull(task_ids='extract_villes_from_frontusers', key='city_country_mapping')
    if city_country_mapping is None:
        logger.error("XCom pull failed: 'city_country_mapping' is None. Check upstream task and XCom keys.")
        raise ValueError("XCom pull failed: 'city_country_mapping' is None.")
    logger.info(f"Received {len(city_country_mapping)} city-country mappings from XCom.")
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    pays_mapping = fetch_pays_data(cursor)
    
    insert_query = """
    INSERT INTO public.dim_ville (ville_id, code_ville, nom_ville, pays_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (nom_ville)
    DO UPDATE SET
        code_ville = EXCLUDED.code_ville,
        nom_ville = EXCLUDED.nom_ville,
        pays_id = EXCLUDED.pays_id;
    """
    
    existing_entries = get_existing_villes(cursor)
    pk_counter = 1
    inserted_count = 0
    for ville, pays in city_country_mapping.items():
        if ville not in existing_entries:
            pays_id = pays_mapping.get(pays.lower())
            if pays_id:
                code = generate_location_code(cursor, existing_entries)
                cursor.execute(insert_query, (pk_counter, code, ville, pays_id))
                inserted_count += 1
                pk_counter += 1
                existing_entries.add(ville)
            else:
                logger.warning(f"Country '{pays}' not found in dim_pays. Skipping city insertion.")
                continue
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{inserted_count} villes et pays insérés ou mis à jour dans PostgreSQL.")

dag = DAG(
    'dag_dim_villes',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_villes_task = PythonOperator(
    task_id='extract_villes_from_frontusers',
    python_callable=extract_villes_from_frontusers,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_villes_and_countries',
    python_callable=load_villes_and_countries_postgres,
    provide_context=True,
    dag=dag,
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting region extraction process..."),
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Region extraction process completed."),
    dag=dag
)

start_task >> extract_villes_task >> load_task >> end_task
