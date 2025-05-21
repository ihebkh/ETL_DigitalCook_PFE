import logging
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    return conn

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["formations"]

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_code_formation(index):
    return f"form{str(index).zfill(4)}"

def generate_primary_key(index):
    return index

def extract_formations_from_mongo():
    try:
        client, formations_col = get_mongo_collections()
        cursor = formations_col.find({"isDeleted": False})
        formations = []
        for doc in cursor:
            formations.append({
                "titreFormation": doc.get("titreFormation", "").strip(),
                "domaine": doc.get("domaine", "").strip()
            })
        client.close()
        cleaned = convert_bson(formations)
        logger.info(f"{len(cleaned)} formations extraites depuis MongoDB.")
        return cleaned
    except Exception as e:
        logger.error(f"Erreur durant l'extraction MongoDB : {e}")
        return []

def transform_formations(raw_formations):
    transformed = []
    seen_titles = set()
    counter = 1
    for doc in raw_formations:
        titre = doc["titreFormation"]
        domaine = doc["domaine"]
        if not titre or titre.lower() in seen_titles:
            continue
        seen_titles.add(titre.lower())
        code_formation = generate_code_formation(counter)
        pk = generate_primary_key(counter)
        transformed.append({
            "formation_id": pk,
            "code_formation": code_formation,
            "titre_formation": titre,
            "domaine_formation": domaine
        })
        counter += 1
    logger.info(f"{len(transformed)} formations transformées avec succès.")
    return transformed

def load_formations_to_postgres(transformed_formations):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    try:
        for formation in transformed_formations:
            record = (
                formation.get('formation_id'),
                formation.get('code_formation'),
                formation.get('titre_formation'),
                formation.get('domaine_formation')
            )
            cursor.execute("""
                INSERT INTO public.dim_formation (
                    formation_id, code_formation, titre_formation, domaine_formation
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (titre_formation) DO UPDATE SET
                    code_formation = EXCLUDED.code_formation,
                    titre_formation = EXCLUDED.titre_formation,
                    domaine_formation = EXCLUDED.domaine_formation;
            """, record)
        conn.commit()
        logger.info(f"{len(transformed_formations)} formations insérées ou mises à jour dans PostgreSQL.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement dans PostgreSQL : {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    dag_id='dag_dim_formations',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

def extract_task():
    return extract_formations_from_mongo()

def transform_task(**kwargs):
    raw_formations = kwargs['ti'].xcom_pull(task_ids='extract_task')
    return transform_formations(raw_formations)

def load_task(**kwargs):
    transformed_formations = kwargs['ti'].xcom_pull(task_ids='transform_task')
    load_formations_to_postgres(transformed_formations)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Début du process d'extraction des formations..."),
    dag=dag
)

extract = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Process d'extraction des formations terminé."),
    dag=dag
)

start_task >> extract >> transform >> load >> end_task