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
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    collection = db["frontusers"]
    logger.info("Connexion MongoDB réussie.")
    return client, collection

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion PostgreSQL réussie.")
    return conn

def get_max_language_pk_and_existing_labels():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(langue_id), 0) FROM dim_langues")
    max_pk = cur.fetchone()[0]
    cur.execute("SELECT nom_langue FROM dim_langues")
    existing_labels = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return max_pk, existing_labels

def generate_langue_code(index):
    return f"LANG{str(index).zfill(3)}"

def extract_from_mongodb(**kwargs):
    client, collection = get_mongodb_connection()
    cursor = collection.find({}, {"_id": 0, "profile.languages": 1, "simpleProfile.languages": 1})

    seen_labels = set()
    languages = []

    for doc in cursor:
        for profile_key in ["profile", "simpleProfile"]:
            languages_list = doc.get(profile_key, {}).get("languages", [])
            if isinstance(languages_list, list):
                for lang in languages_list:
                    if isinstance(lang, dict):
                        label = lang.get("label", "").strip()
                        level = lang.get("level", "").strip()
                        if label and level and label not in seen_labels:
                            languages.append({"label": label, "level": level})
                            seen_labels.add(label)

    client.close()
    kwargs['ti'].xcom_push(key='extracted_languages', value=languages)
    logger.info(f"{len(languages)} langues extraites.")

def transform_languages(**kwargs):
    languages = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='extracted_languages')
    max_pk, existing_labels = get_max_language_pk_and_existing_labels()
    transformed = []
    compteur = max_pk

    for lang in languages:
        if lang["label"] not in existing_labels:
            compteur += 1
            transformed.append({
                "langue_pk": compteur,
                "langue_code": generate_langue_code(compteur),
                "label": lang["label"],
                "level": lang["level"]
            })
            existing_labels.add(lang["label"])

    kwargs['ti'].xcom_push(key='transformed_languages', value=transformed)
    logger.info(f"{len(transformed)} nouvelles langues transformées.")

def load_languages(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_languages', key='transformed_languages')
    if not data:
        logger.info("Aucune donnée à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_langues (
        langue_id, code_langue, nom_langue, niveau_langue
    ) VALUES (%s, %s, %s, %s)
    ON CONFLICT (langue_id) DO UPDATE SET
        code_langue = EXCLUDED.code_langue,
        nom_langue = EXCLUDED.nom_langue,
        niveau_langue = EXCLUDED.niveau_langue;
    """

    for record in data:
        cur.execute(insert_query, (
            record["langue_pk"],
            record["langue_code"],
            record["label"],
            record["level"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} lignes insérées/mises à jour dans PostgreSQL.")

dag = DAG(
    dag_id='dag_dim_languages',
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
    task_id='transform_languages',
    python_callable=transform_languages,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_languages',
    python_callable=load_languages,
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


start_task>>extract_task >> transform_task >> load_task>>end_task
