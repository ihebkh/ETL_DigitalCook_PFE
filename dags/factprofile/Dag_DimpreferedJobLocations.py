import json
import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import tempfile
from bson import ObjectId

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

def generate_location_code(existing_codes):
    if not existing_codes:
        return "LOC001"
    else:
        last_number = max(int(code.replace("LOC", "")) for code in existing_codes if code.startswith("LOC"))
        new_number = last_number + 1
        return f"LOC{str(new_number).zfill(3)}"

def handle_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

def extract_from_mongodb(**kwargs):
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = list(collection.find({}, {"_id": 0, "profile.preferedJobLocations": 1, "simpleProfile.preferedJobLocations": 1}))
        for i in range(len(mongo_data)):
            mongo_data[i] = json.loads(json.dumps(mongo_data[i], default=handle_objectid))

        client.close()

        logger.info(f"Extracted {len(mongo_data)} records from MongoDB.")
        with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
            temp_file_path = temp_file.name
            json.dump(mongo_data, temp_file, ensure_ascii=False, indent=4)

        kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)
        return mongo_data
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def transform_data_from_temp_file(**kwargs):
    try:
        temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='temp_file_path')

        with open(temp_file_path, 'r', encoding='utf-8') as file:
            mongo_data = json.load(file)

        job_locations = []
        existing_entries = set()

        for user in mongo_data:
            for profile_key in ["profile", "simpleProfile"]:
                if isinstance(user, dict) and profile_key in user and isinstance(user[profile_key], dict):
                    profile = user[profile_key]
                    if "preferedJobLocations" in profile:
                        locations = profile["preferedJobLocations"]

                        if isinstance(locations, list):
                            for loc in locations:
                                if isinstance(loc, dict):
                                    pays = loc.get("pays", "").strip()
                                    ville = loc.get("ville", "").strip()
                                    region = loc.get("region", "").strip()

                                    if not (pays or ville or region):
                                        continue

                                    location_tuple = (pays, ville, region)
                                    if location_tuple not in existing_entries:
                                        existing_entries.add(location_tuple)
                                        job_locations.append({
                                            "preferedJobLocationsCode": None,
                                            "pays": pays,
                                            "ville": ville,
                                            "region": region
                                        })

        logger.info(f"Transformed {len(job_locations)} job locations.")
        kwargs['ti'].xcom_push(key='job_locations', value=job_locations)
        return job_locations
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_into_postgres(**kwargs):
    try:
        job_locations = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='job_locations')

        if not job_locations:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgres_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO Dim_preferedJobLocations (preferedJobLocationsCode, pays, ville, region)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (preferedjoblocations_pk) 
        DO UPDATE SET 
            pays = EXCLUDED.pays,
            ville = EXCLUDED.ville,
            region = EXCLUDED.region;
        """

        cur.execute("SELECT preferedJobLocationsCode, pays, ville, region FROM Dim_preferedJobLocations")
        existing_entries = {(row[1], row[2], row[3]): row[0] for row in cur.fetchall()}

        inserted_count = 0

        for record in job_locations:
            if not (record["pays"].strip() or record["ville"].strip() or record["region"].strip()):
                logger.info("Skipped empty job location entry.")
                continue

            location_tuple = (record["pays"], record["ville"], record["region"])
            if location_tuple in existing_entries:
                record["preferedJobLocationsCode"] = existing_entries[location_tuple]
            else:
                if record["preferedJobLocationsCode"] is None:
                    record["preferedJobLocationsCode"] = generate_location_code(existing_entries.values())
                existing_entries[location_tuple] = record["preferedJobLocationsCode"]

            values = (
                record["preferedJobLocationsCode"],
                record["pays"],
                record["ville"],
                record["region"]
            )
            cur.execute(insert_query, values)
            inserted_count += 1

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"{inserted_count} job locations inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading job locations into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimpreferedJobLocations',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data_from_temp_file',
    python_callable=transform_data_from_temp_file,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task