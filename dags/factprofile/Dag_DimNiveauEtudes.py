import logging
from pymongo import MongoClient
import tempfile
import json
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
        MONGO_COLLECTION = "frontusers"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def safe_int(value):
    try:
        return int(value) if value and isinstance(value, (str, int)) and str(value).isdigit() else None
    except ValueError:
        return None

def generate_diplome_code(existing_codes):
    if existing_codes:
        existing_numbers = [int(code.replace("DIP", "")) for code in existing_codes if isinstance(code, str) and code.startswith("DIP")]
        if existing_numbers:
            last_number = max(existing_numbers) + 1
        else:
            last_number = 1
    else:
        last_number = 1
    new_code = f"DIP{str(last_number).zfill(3)}"
    existing_codes.add(new_code)
    return new_code

def extract_from_mongodb(**kwargs):
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = collection.find({}, {"_id": 0, "profile.niveauDetudes": 1, "simpleProfile.niveauDetudes": 1})

        niveaux_etudes = []
        for user in mongo_data:
            if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
                niveau_list = user["profile"].get("niveauDetudes", [])
                if isinstance(niveau_list, list):
                    for niveau in niveau_list:
                        if isinstance(niveau, dict) and niveau.get("label"):
                            niveaux_etudes.append({
                                "diplome_code": None,  
                                "label": niveau.get("label"),
                                "universite": niveau.get("universite"),
                                "start_year": safe_int(niveau.get("du", {}).get("year", "")),
                                "start_month": safe_int(niveau.get("du", {}).get("month", "")),
                                "end_year": safe_int(niveau.get("au", {}).get("year", "")),
                                "end_month": safe_int(niveau.get("au", {}).get("month", "")),
                                "nom_diplome": niveau.get("nomDiplome"),
                                "pays": niveau.get("pays"),
                            })

            if isinstance(user, dict) and "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
                niveau_list = user["simpleProfile"].get("niveauDetudes", [])
                if isinstance(niveau_list, list):
                    for niveau in niveau_list:
                        if isinstance(niveau, dict) and niveau.get("label"):
                            niveaux_etudes.append({
                                "diplome_code": None,  
                                "label": niveau.get("label"),
                                "universite": niveau.get("universite"),
                                "start_year": safe_int(niveau.get("du", {}).get("year", "")),
                                "start_month": safe_int(niveau.get("du", {}).get("month", "")),
                                "end_year": safe_int(niveau.get("au", {}).get("year", "")),
                                "end_month": safe_int(niveau.get("au", {}).get("month", "")),
                                "nom_diplome": niveau.get("nomDiplome"),
                                "pays": niveau.get("pays"),
                            })

        client.close()

        logger.info(f"Extracted {len(niveaux_etudes)} study levels.")
        
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
            temp_file_path = temp_file.name
            json.dump(niveaux_etudes, temp_file, ensure_ascii=False, indent=4)
        
        kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
        return niveaux_etudes
    except Exception as e:
        logger.error(f"Error extracting study levels from MongoDB: {e}")
        raise

def load_into_postgres(**kwargs):
    try:
        temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='temp_file_path')

        with open(temp_file_path, 'r', encoding='utf-8') as file:
            niveaux_etudes = json.load(file)

        if not niveaux_etudes:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_niveau_d_etudes (diplome_code, label, universite, start_year, start_month, end_year, end_month, nom_diplome, pays)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (diplome_code) DO UPDATE 
        SET label = EXCLUDED.label, universite = EXCLUDED.universite, start_year = EXCLUDED.start_year, start_month = EXCLUDED.start_month, 
        end_year = EXCLUDED.end_year, end_month = EXCLUDED.end_month, nom_diplome = EXCLUDED.nom_diplome, pays = EXCLUDED.pays;
        """

        cur.execute("SELECT diplome_code FROM dim_niveau_d_etudes")
        existing_codes = {row[0] for row in cur.fetchall()}

        for record in niveaux_etudes:
            if not any([record.get("label"), record.get("universite"), record.get("nom_diplome")]):
                logger.info(f"Skipping record with missing essential data: {record}")
                continue

            if record["diplome_code"] is None:  
                record["diplome_code"] = generate_diplome_code(existing_codes)
                existing_codes.add(record["diplome_code"])  

            values = (
                record["diplome_code"],
                record["label"],
                record["universite"],
                record["start_year"],
                record["start_month"],
                record["end_year"],
                record["end_month"],
                record["nom_diplome"],
                record["pays"],
            )

            cur.execute("SELECT 1 FROM dim_niveau_d_etudes WHERE diplome_code = %s", (record["diplome_code"],))

            if cur.fetchone():
                logger.info(f"Updating diploma {record['diplome_code']}: {values}")
                cur.execute(insert_query, values)
            else:
                logger.info(f"Inserting diploma {record['diplome_code']}: {values}")
                cur.execute(insert_query, values)

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"{len(niveaux_etudes)} study levels inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading study levels into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimNiveauEtudes',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task
