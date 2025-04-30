import logging
from pymongo import MongoClient
from bson import ObjectId
import datetime
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = client["PowerBi"]
    collection = mongo_db["formations"]
    return client, mongo_db, collection

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return obj

def generate_code_formation(pk):
    return f"formation{str(pk).zfill(4)}"

def load_existing_formations():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT formation_id, titre_formation
        FROM public.dim_formation
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    mapping = {}
    for formation_id, titre in rows:
        key = (titre or '')
        mapping[key] = formation_id
    return mapping

def get_max_formation_pk():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(formation_id), 0) FROM public.dim_formation;")
    max_pk = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return max_pk

def extract_formations(**kwargs):
    client, mongo_db, collection = get_mongodb_connection()
    formations = list(collection.find({}, {
        "_id": 0, "titreFormation": 1, "domaine": 1
    }))
    client.close()
    formations = convert_bson(formations)
    kwargs['ti'].xcom_push(key='formations', value=formations)
    logger.info(f"{len(formations)} formations extraites de MongoDB.")

def load_formations(**kwargs):
    formations = kwargs['ti'].xcom_pull(task_ids='extract_formations', key='formations')
    if not formations:
        logger.info("Aucune formation à charger.")
        return

    existing_formations = load_existing_formations()
    current_pk = get_max_formation_pk()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    query_upsert = """
    INSERT INTO public.dim_formation (
        formation_id, code_formation, titre_formation, domaine_formation
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (titre_formation) DO UPDATE SET
        code_formation = EXCLUDED.code_formation,
        titre_formation = EXCLUDED.titre_formation,
        domaine_formation = EXCLUDED.domaine_formation;
    """

    for formation in formations:
        titre = formation.get("titreFormation", "")
        domaine = formation.get("domaine", "")

        key = (titre or '')

        if key in existing_formations:
            formation_id = existing_formations[key]
        else:
            current_pk += 1
            formation_id = current_pk

        code = generate_code_formation(formation_id)

        data = (
            formation_id, code, titre, domaine
        )

        cursor.execute(query_upsert, data)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(formations)} formations insérées ou mises à jour.")

dag = DAG(
    dag_id='dag_dim_formation',
    start_date=dt(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_formations',
    python_callable=extract_formations,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_formations',
    python_callable=load_formations,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
