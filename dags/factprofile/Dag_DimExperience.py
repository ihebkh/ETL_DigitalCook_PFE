import logging
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = client["PowerBi"]
    collection = mongo_db["frontusers"]
    secteur_collection = mongo_db["secteurdactivities"]
    return client, mongo_db, collection, secteur_collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_max_experience_pk():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(experience_pk), 0) FROM dim_experience")
    max_pk = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return max_pk

def load_dim_secteur():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT secteur_pk, label FROM public.dim_secteur;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {label.lower(): pk for pk, label in rows if label is not None}

def load_dim_metier():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT metier_pk, label_jobs FROM public.dim_metier;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {label.lower(): pk for pk, label in rows if label is not None}

def is_valid_objectid(value):
    try:
        ObjectId(value)
        return True
    except InvalidId:
        return False

def generate_code_experience(index):
    return f"EXPR{index:04d}"

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
    label_to_pk_secteur = load_dim_secteur()
    label_to_pk_metier = load_dim_metier()
    documents = collection.find()
    filtered_experiences = []

    current_pk = get_max_experience_pk()
    code_index = current_pk + 1

    for doc in documents:
        for profile_field in ['profile', 'simpleProfile']:
            if profile_field in doc and 'experiences' in doc[profile_field]:
                for experience in doc[profile_field]['experiences']:
                    if isinstance(experience, dict):
                        filtered_experience = {
                            "role": experience.get("role", ""),
                            "entreprise": experience.get("entreprise", ""),
                            "du_month": experience.get("du", {}).get("month", ""),
                            "du_year": experience.get("du", {}).get("year", ""),
                            "au_month": experience.get("au", {}).get("month", ""),
                            "au_year": experience.get("au", {}).get("year", ""),
                            "ville": experience.get("ville", {}).get("value", "") if isinstance(experience.get("ville", {}), dict) else experience.get("ville", ""),
                            "pays": experience.get("pays", {}).get("value", "") if isinstance(experience.get("pays", {}), dict) else experience.get("pays", ""),
                            "typeContrat": experience.get("typeContrat", {}).get("value", "") if isinstance(experience.get("typeContrat", {}), dict) else experience.get("typeContrat", ""),
                            "secteur": experience.get("secteur", ""),
                            "metier": experience.get("metier", "")
                        }

                        values = [filtered_experience[k] for k in filtered_experience]
                        if not any(str(v).strip() for v in values):
                            continue

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
                        filtered_experience["experience_pk"] = code_index
                        filtered_experience["code_experience"] = generate_code_experience(code_index)
                        filtered_experiences.append(convert_bson(filtered_experience))
                        code_index += 1

    client.close()
    kwargs['ti'].xcom_push(key='dim_experiences', value=filtered_experiences)
    logger.info(f"{len(filtered_experiences)} expériences extraites.")
    return filtered_experiences

def insert_experiences_into_postgres(**kwargs):
    experiences = kwargs['ti'].xcom_pull(task_ids='extract_dim_experience', key='dim_experiences')
    conn = get_postgres_connection()
    cursor = conn.cursor()

    upsert_query = """
        INSERT INTO public.dim_experience (
            experience_pk, codeexperience, role, entreprise,
            start_year, start_month, end_year, end_month,
            pays, ville, typecontrat,
            fk_secteur, fk_metier
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codeexperience) DO UPDATE SET
            role = EXCLUDED.role,
            entreprise = EXCLUDED.entreprise,
            start_year = EXCLUDED.start_year,
            start_month = EXCLUDED.start_month,
            end_year = EXCLUDED.end_year,
            end_month = EXCLUDED.end_month,
            pays = EXCLUDED.pays,
            ville = EXCLUDED.ville,
            typecontrat = EXCLUDED.typecontrat,
            fk_secteur = EXCLUDED.fk_secteur,
            fk_metier = EXCLUDED.fk_metier;
    """

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
            int(exp["secteur"]) if str(exp["secteur"]).isdigit() else None,
            int(exp["metier"]) if str(exp["metier"]).isdigit() else None
        ))

    conn.commit()
    cursor.close()
    conn.close()
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

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)
# en parallele
[wait_dim_secteur, wait_dim_metier] >> extract_task

extract_task >> load_task >> end_task
