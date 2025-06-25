import logging
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["PowerBi"]
    collection = db["frontusers"]
    return client, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_max_client_pk(conn):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(client_id), 0) FROM dim_client")
    max_pk = cur.fetchone()[0]
    cur.close()
    return max_pk

def convert_bson(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, dict):
        return {k: convert_bson(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_bson(v) for v in value]
    return value

def load_dim_pays(conn):
    cur = conn.cursor()
    cur.execute("SELECT pays_id, nom_pays_en FROM public.dim_pays;")
    rows = cur.fetchall()
    cur.close()
    return {label.lower(): pk for pk, label in rows if label}

def extract_data(**kwargs):
    client, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {"_id": 0}))
    client.close()
    clean_data = [convert_bson(doc) for doc in mongo_data]
    kwargs['ti'].xcom_push(key='mongo_data', value=clean_data)
    logger.info(f"{len(clean_data)} documents extraits de MongoDB.")

def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='mongo_data')
    if not raw_data:
        logger.warning("Aucune donnée brute reçue pour transformation.")
        return

    seen_matricules = set()
    transformed = []
    conn = get_postgres_connection()
    max_pk = get_max_client_pk(conn)
    pays_map = load_dim_pays(conn)
    skipped_no_matricule = 0
    skipped_duplicates = 0

    for record in raw_data:
        matricule = record.get("matricule")
        if not matricule:
            skipped_no_matricule += 1
            logger.warning(f"Document ignoré (sans matricule): {record}")
            continue
        if matricule in seen_matricules:
            skipped_duplicates += 1
            logger.warning(f"Document ignoré (doublon matricule): {matricule}")
            continue
        seen_matricules.add(matricule)
        max_pk += 1
        profile_data = record.get("profile") or record.get("simpleProfile") or {}
        pays_label = (profile_data.get("pays") or "").strip().lower()
        pays_id = pays_map.get(pays_label) if pays_label else None
        transformed.append({
            "client_pk": max_pk,
            "matricule": matricule,
            "nom": record.get("nom"),
            "prenom": record.get("prenom"),
            "birthdate": profile_data.get("birthDate"),
            "nationality": profile_data.get("nationality"),
            "pays": pays_id,
            "situation": profile_data.get("situation"),
            "etatcivile": profile_data.get("etatCivil"),
            "photo": record.get("google_Photo", profile_data.get("google_Photo")),
            "niveau_etude_actuelle": profile_data.get("niveauDetudeActuel"),
            "gender": profile_data.get("gender"),
            "profileType": profile_data.get("profileType"),
        })

    conn.close()
    logger.info(f"Clients transformés : {len(transformed)}")
    logger.info(f"Documents ignorés faute de matricule : {skipped_no_matricule}")
    logger.info(f"Documents ignorés (doublons matricule) : {skipped_duplicates}")

    kwargs['ti'].xcom_push(key='transformed_clients', value=transformed)

def get_date_id_from_dim_dates(date_value, conn):
    cur = conn.cursor()
    formatted_date = None
    if isinstance(date_value, str):
        try:
            dt = datetime.fromisoformat(date_value)
            formatted_date = dt.strftime('%Y-%m-%d')
        except Exception:
            formatted_date = date_value
    elif isinstance(date_value, datetime):
        formatted_date = date_value.strftime('%Y-%m-%d')

    if not formatted_date:
        cur.close()
        return None, None

    cur.execute("SELECT date_id, code_date FROM public.dim_dates WHERE code_date = %s", (formatted_date,))
    date_data = cur.fetchone()
    cur.close()
    if date_data:
        return date_data[0], date_data[1]
    return None, None

def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_clients')
    if not data:
        logger.warning("Aucune donnée à insérer.")
        return
    conn = get_postgres_connection()
    cur = conn.cursor()
    insert_query = """
    INSERT INTO dim_client (
        client_id, matricule_client, nom_client, prenom_client, date_naissance, nationalite,
        pays_residence, situation, etat_civil, photo_client, niveau_etudes_actuel,sexe,type_de_profil
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s ,%s,%s)
    ON CONFLICT (matricule_client) DO UPDATE SET
        matricule_client = EXCLUDED.matricule_client,
        nom_client = EXCLUDED.nom_client,
        prenom_client = EXCLUDED.prenom_client,
        date_naissance = EXCLUDED.date_naissance,
        nationalite = EXCLUDED.nationalite,
        pays_residence = EXCLUDED.pays_residence,
        situation = EXCLUDED.situation,
        etat_civil = EXCLUDED.etat_civil,
        photo_client = EXCLUDED.photo_client,
        niveau_etudes_actuel = EXCLUDED.niveau_etudes_actuel,
        sexe = EXCLUDED.sexe,
        type_de_profil = EXCLUDED.type_de_profil
    """
    inserted_count = 0
    for row in data:
        birthdate = row.get("birthdate")
        date_id = None
        if birthdate:
            date_id, _ = get_date_id_from_dim_dates(birthdate, conn)
        # On insère None (NULL en PG) si date_id introuvable ou birthdate manquante
        cur.execute(insert_query, (
            row["client_pk"],
            row["matricule"],
            row.get("nom"),
            row.get("prenom"),
            date_id,
            row.get("nationality"),
            row.get("pays"),
            row.get("situation"),
            row.get("etatcivile"),
            row.get("photo"),
            row.get("niveau_etude_actuelle"),
            row.get("gender"),
            row.get("profileType")
        ))
        inserted_count += 1
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{inserted_count} clients insérés/mis à jour dans PostgreSQL.")

dag = DAG(
    dag_id='dag_dim_Clients',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
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

start_task>>extract_task >> transform_task >> load_task>>end_task
