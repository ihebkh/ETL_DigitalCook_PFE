import logging
import re
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_value):
    if isinstance(date_value, dict):
        year = date_value.get("year")
        month = date_value.get("month")
        year = int(year) if isinstance(year, (int, str)) and str(year).isdigit() else None
        month = int(month) if isinstance(month, (int, str)) and str(month).isdigit() else None
        return year, month
    elif isinstance(date_value, str):
        if re.match(r"^\d{4}-\d{2}$", date_value):
            year, month = map(int, date_value.split("-"))
            return year, month
        elif re.match(r"^\d{4}$", date_value):
            return int(date_value), None
    return None, None

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = client["PowerBi"]
    collection = mongo_db["frontusers"]
    secteur_collection = mongo_db["secteurdactivities"]
    return client, mongo_db, collection, secteur_collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def load_dim_secteur(cursor):
    cursor.execute("SELECT secteur_id, nom_secteur FROM public.dim_secteur;")
    rows = cursor.fetchall()
    return {label.lower(): pk for pk, label in rows if label}

def load_dim_metier(cursor):
    cursor.execute("SELECT metier_id, nom_metier FROM public.dim_metier;")
    rows = cursor.fetchall()
    return {label.lower(): pk for pk, label in rows if label}

def load_existing_experiences(cursor):
    cursor.execute("""
        SELECT experience_id, role_experience, nom_entreprise, annee_debut, mois_debut, annee_fin, mois_fin
        FROM public.dim_experience
    """)
    rows = cursor.fetchall()
    mapping = {}
    for exp_id, role, entreprise, annee_debut, mois_debut, annee_fin, mois_fin in rows:
        key = (role or '') + (entreprise or '') + str(annee_debut or '') + str(mois_debut or '') + str(annee_fin or '') + str(mois_fin or '')
        mapping[key] = exp_id
    return mapping

def get_max_experience_pk(cursor):
    cursor.execute("SELECT COALESCE(MAX(experience_id), 0) FROM dim_experience;")
    return cursor.fetchone()[0]

def is_valid_objectid(value):
    try:
        ObjectId(value)
        return True
    except InvalidId:
        return False

def generate_code_experience(pk):
    return f"EXPR{pk:04d}"

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def extract_experiences(**kwargs):
    client, _, collection, secteur_collection = get_mongodb_connection()
    conn = get_postgres_connection()
    with conn:
        with conn.cursor() as cursor:
            label_to_pk_secteur = load_dim_secteur(cursor)
            label_to_pk_metier = load_dim_metier(cursor)
            existing_experiences = load_existing_experiences(cursor)
            current_pk = get_max_experience_pk(cursor)

            documents = collection.find()
            filtered_experiences = []

            for doc in documents:
                for profile_field in ['profile', 'simpleProfile']:
                    if profile_field in doc and isinstance(doc[profile_field], dict):
                        experiences = doc[profile_field].get('experiences')

                        if isinstance(experiences, list):
                            for experience in experiences:
                                if isinstance(experience, dict):
                                    role = experience.get("role", "") or experience.get("poste", "")
                                    entreprise = experience.get("entreprise", "")
                                    ville = experience.get("ville", {}).get("value", "") if isinstance(experience.get("ville", {}), dict) else experience.get("ville", "")
                                    pays = experience.get("pays", {}).get("value", "") if isinstance(experience.get("pays", {}), dict) else experience.get("pays", "")
                                    type_contrat = experience.get("typeContrat", {}).get("value", "") if isinstance(experience.get("typeContrat", {}), dict) else experience.get("typeContrat", "")
                                    secteur = experience.get("secteur", "")
                                    metier = experience.get("metier", "")

                                    if not role and not ville and not type_contrat:
                                        continue

                                    start_year, start_month = parse_date(experience.get("du", ""))
                                    end_year, end_month = parse_date(experience.get("au", ""))

                                    filtered_experience = {
                                        "role": role,
                                        "entreprise": entreprise,
                                        "ville": ville,
                                        "pays": pays,
                                        "typeContrat": type_contrat,
                                        "secteur": secteur,
                                        "metier": metier,
                                        "du_year": start_year,
                                        "du_month": start_month,
                                        "au_year": end_year,
                                        "au_month": end_month
                                    }

                                    key = (role or '') + (entreprise or '') + str(start_year or '') + str(start_month or '') + str(end_year or '') + str(end_month or '')
                                    if key in existing_experiences:
                                        experience_pk = existing_experiences[key]
                                    else:
                                        current_pk += 1
                                        experience_pk = current_pk

                                    secteur_id = filtered_experience["secteur"]
                                    secteur_doc = None
                                    if secteur_id:
                                        secteur_doc = secteur_collection.find_one({"_id": secteur_id})
                                        if secteur_doc:
                                            secteur_label = secteur_doc.get("label", "").lower()
                                            filtered_experience["secteur"] = label_to_pk_secteur.get(secteur_label, None)

                                    metier_ids = filtered_experience["metier"]
                                    metier_pk_list = []
                                    if metier_ids:
                                        if isinstance(metier_ids, str):
                                            metier_ids = [metier_ids]
                                        metier_ids = [ObjectId(m) for m in metier_ids if is_valid_objectid(m)]
                                        if secteur_doc and "jobs" in secteur_doc:
                                            for job in secteur_doc["jobs"]:
                                                if job["_id"] in metier_ids:
                                                    label = job.get("label", "").lower()
                                                    metier_pk = label_to_pk_metier.get(label)
                                                    if metier_pk:
                                                        metier_pk_list.append(str(metier_pk))

                                    filtered_experience["metier"] = metier_pk_list[0] if metier_pk_list else None
                                    filtered_experience["experience_pk"] = experience_pk
                                    filtered_experience["code_experience"] = generate_code_experience(experience_pk)

                                    filtered_experiences.append(convert_bson(filtered_experience))
                        else:
                            logger.warning(f"Aucune expérience valide dans '{profile_field}' pour user {doc.get('_id')}")
    client.close()
    conn.close()
    kwargs['ti'].xcom_push(key='dim_experiences', value=filtered_experiences)
    logger.info(f"{len(filtered_experiences)} expériences extraites.")
    return filtered_experiences

def insert_experiences_into_postgres(**kwargs):
    experiences = kwargs['ti'].xcom_pull(task_ids='extract_dim_experience', key='dim_experiences')
    hook = PostgresHook(postgres_conn_id='postgres')
    upsert_query = """
        INSERT INTO public.dim_experience (
            experience_id, code_experience, role_experience, nom_entreprise,
            annee_debut, mois_debut, annee_fin, mois_fin,
            pays_experience, ville_experience, type_contrat,
            secteur_id, metier_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (experience_id) DO UPDATE SET
            code_experience = EXCLUDED.code_experience,
            role_experience = EXCLUDED.role_experience,
            nom_entreprise = EXCLUDED.nom_entreprise,
            annee_debut = EXCLUDED.annee_debut,
            mois_debut = EXCLUDED.mois_debut,
            annee_fin = EXCLUDED.annee_fin,
            mois_fin = EXCLUDED.mois_fin,
            pays_experience = EXCLUDED.pays_experience,
            ville_experience = EXCLUDED.ville_experience,
            type_contrat = EXCLUDED.type_contrat,
            secteur_id = EXCLUDED.secteur_id,
            metier_id = EXCLUDED.metier_id;
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            
            for exp in experiences:
                cursor.execute(upsert_query, (
                    exp["experience_pk"],
                    exp["code_experience"],
                    exp["role"],
                    exp["entreprise"],
                    int(exp["du_year"]) if exp["du_year"] else None,
                    int(exp["du_month"]) if exp["du_month"] else None,
                    int(exp["au_year"]) if exp["au_year"] else None,
                    int(exp["au_month"]) if exp["au_month"] else None,
                    exp["pays"],
                    exp["ville"],
                    exp["typeContrat"],
                    int(exp["secteur"]) if exp["secteur"] and str(exp["secteur"]).isdigit() else None,
                    int(exp["metier"]) if exp["metier"] and str(exp["metier"]).isdigit() else None
                ))
            conn.commit()
    logger.info(f"{len(experiences)} expériences insérées ou mises à jour.")

dag = DAG(
    dag_id='dag_dim_experience',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

wait_dim_secteur = ExternalTaskSensor(
    task_id='wait_for_dim_secteur',
    external_dag_id='dag_dim_secteur',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_metier = ExternalTaskSensor(
    task_id='wait_for_dim_metier',
    external_dag_id='Dag_Metier',
    external_task_id='load_jobs_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_dim_experience',
    python_callable=extract_experiences,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_dim_experience',
    python_callable=insert_experiences_into_postgres,
    provide_context=True,
    dag=dag
)

[wait_dim_secteur, wait_dim_metier] >> extract_task >> load_task
