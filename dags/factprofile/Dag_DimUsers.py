import tempfile
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson import ObjectId
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    logger.info("Connexion à MongoDB réussie.")
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion à PostgreSQL réussie.")
    return conn

def extract_from_mongodb_to_temp_file(**kwargs):
    logger.info("Extraction des données de MongoDB...")
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))

    def datetime_converter(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError("Type non sérialisable : {}".format(type(obj)))

    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        json.dump(mongo_data, temp_file, ensure_ascii=False, indent=4, default=datetime_converter)
        temp_file_path = temp_file.name
    client.close()
    
    logger.info(f"Données extraites et sauvegardées dans le fichier temporaire: {temp_file_path}")
    
    kwargs['ti'].xcom_push(key='temp_file_path', value=temp_file_path)

def transform_data_from_temp_file(**kwargs):
    temp_file_path = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb_to_temp_file', key='temp_file_path')
    logger.info(f"Transformation des données depuis le fichier temporaire: {temp_file_path}")
    
    with open(temp_file_path, 'r', encoding='utf-8') as file:
        mongo_data = json.load(file)

    seen_matricules = set()
    transformed_data = []

    for record in mongo_data:
        matricule = record.get("matricule")
        if matricule in seen_matricules:
            continue 
        seen_matricules.add(matricule)

        transformed_data.append({
            "matricule": matricule,
            "nom": record.get("nom"),
            "prenom": record.get("prenom"),
            "birthdate": record.get("profile", {}).get("birthDate"),
            "nationality": record.get("profile", {}).get("nationality"),
            "adresseDomicile": record.get("profile", {}).get("adresseDomicile"),
            "pays": record.get("profile", {}).get("pays"),
            "situation": record.get("profile", {}).get("situation"),
            "etatcivile": record.get("profile", {}).get("etatCivil"),
            "photo": record.get("google_Photo", record.get("profile", {}).get("google_Photo", None)),
            "intituleposte": record.get("profile", {}).get("intituleposte", None),
            "niveau_etude_actuelle": record.get("profile", {}).get("niveau_etude_actuelle", None)
        })
    
    logger.info(f"Transformation terminée, {len(transformed_data)} clients traités.")
    return transformed_data

def load_into_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_data_from_temp_file', key='return_value')
    
    if not data:
        logger.error("No data to insert into PostgreSQL.")
        return
    
    logger.info("Chargement des données dans PostgreSQL...")
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_client (matricule, nom, prenom, birthdate, nationality, adresseDomicile, pays, situation, etatcivile, photo, intituleposte, niveau_etude_actuelle)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (matricule) DO UPDATE SET 
        nom = EXCLUDED.nom,
        prenom = EXCLUDED.prenom,
        birthdate = EXCLUDED.birthdate,
        nationality = EXCLUDED.nationality,
        adresseDomicile = EXCLUDED.adresseDomicile,
        pays = EXCLUDED.pays,
        situation = EXCLUDED.situation,
        etatcivile = EXCLUDED.etatcivile,
        photo = EXCLUDED.photo,
        intituleposte = EXCLUDED.intituleposte,
        niveau_etude_actuelle = EXCLUDED.niveau_etude_actuelle
    """
    
    for record in data:
        values = (
            record.get("matricule"),
            record.get("nom", "").strip() if record.get("nom") else None,
            record.get("prenom", "").strip() if record.get("prenom") else None,
            record.get("birthdate") if record.get("birthdate") else None,
            record.get("nationality", "").strip() if record.get("nationality") else None,
            record.get("adresseDomicile", "").strip() if record.get("adresseDomicile") else None,
            record.get("pays", "").strip() if record.get("pays") else None,
            record.get("situation", "").strip() if record.get("situation") else None,
            record.get("etatcivile", "").strip() if record.get("etatcivile") else None,
            record.get("photo", "").strip() if record.get("photo") else None,
            record.get("intituleposte", "").strip() if record.get("intituleposte") else None,
            record.get("niveau_etude_actuelle", "").strip() if record.get("niveau_etude_actuelle") else None
        )
        
        # Log the values to debug
        logger.debug(f"Inserting record: {values}")
        
        try:
            cur.execute(insert_query, values)
        except Exception as e:
            logger.error(f"Error inserting record: {values} - Error: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} lignes insérées dans PostgreSQL.")


dag = DAG(
    'Dag_DimClients',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb_to_temp_file',
    python_callable=extract_from_mongodb_to_temp_file,
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
