import logging
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion à PostgreSQL réussie.")
    return conn

def get_existing_competences():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT nom_competence FROM dim_competence")
    competences = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return competences

def get_next_competence_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(competence_id), 0) FROM dim_competence")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk + 1

def extract_from_mongodb(**kwargs):
    client, _, collection = get_mongodb_connection()
    mongo_data = list(collection.find({}, {
        "_id": 0, 
        "profile.competenceGenerales": 1, 
        "simpleProfile.competenceGenerales": 1, 
    }))
    
    competencies = set()
    for user in mongo_data:
        if "profile" in user:
            competencies.update(user["profile"].get("competenceGenerales", []))
        if "simpleProfile" in user:
            competencies.update(user["simpleProfile"].get("competenceGenerales", []))

    unique_competencies = list(competencies)
    kwargs['ti'].xcom_push(key='extracted_competencies', value=unique_competencies)
    client.close()
    logger.info(f"{len(unique_competencies)} compétences extraites de MongoDB")

def transform_competences(**kwargs):
    extracted_competencies = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='extracted_competencies')
    existing_competences = get_existing_competences()
    next_pk = get_next_competence_pk()
    transformed_data = []

    for comp in extracted_competencies:
        comp = comp.strip() if comp else None
        if comp and comp not in existing_competences:
            transformed_data.append({
                "competence_pk": next_pk,
                "competence_code": f"COMP{str(next_pk).zfill(3)}",
                "competence_name": comp
            })
            next_pk += 1
            existing_competences.add(comp)

    kwargs['ti'].xcom_push(key='transformed_competences', value=transformed_data)
    logger.info(f"{len(transformed_data)} nouvelles compétences à insérer")

def load_competences(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_competences', key='transformed_competences')
    if not transformed_data:
        logger.info("Aucune nouvelle compétence à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_competence (competence_id, code_competence, nom_competence)
    VALUES (%s, %s, %s)
    ON CONFLICT (competence_id)DO UPDATE SET
        code_competence = EXCLUDED.code_competence,
        nom_competence = EXCLUDED.nom_competence;
    """

    for record in transformed_data:
        cur.execute(insert_query, (record["competence_pk"], record["competence_code"], record["competence_name"]))
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(transformed_data)} lignes insérées dans PostgreSQL")

dag = DAG(
    'dag_dim_Competences',
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
    task_id='transform_competences',
    python_callable=transform_competences,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_competences',
    python_callable=load_competences,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
