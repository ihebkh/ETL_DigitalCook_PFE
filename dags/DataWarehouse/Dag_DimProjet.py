import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def get_mongodb_connection():
    try:
        mongo_uri = Variable.get("MONGO_URI")
        client = MongoClient(mongo_uri)
        collection = client["PowerBi"]["frontusers"]
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        raise


def get_postgresql_connection(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()

def get_existing_projects(conn_id):
    with get_postgresql_connection(conn_id) as conn:
        cur = conn.cursor()
        cur.execute("SELECT projet_id, nom_projet, code_projet FROM dim_projet")
        existing = {(row[1]): (row[0], row[2]) for row in cur.fetchall()}
        cur.close()
    return existing

def get_max_project_id(conn_id):
    with get_postgresql_connection(conn_id) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(projet_id), 0) FROM dim_projet")
        max_pk = cur.fetchone()[0]
        cur.close()
    return max_pk

def generate_project_code(pk):
    return f"PROJ{str(pk).zfill(3)}"

def extract_from_mongodb(**kwargs):
    client, collection = get_mongodb_connection()
    cursor = collection.find({}, {"_id": 0, "profile.projets": 1, "simpleProfile.projets": 1})
    projects = []

    for user in cursor:
        for profile_key in ['profile', 'simpleProfile']:
            projets = user.get(profile_key, {}).get("projets", [])
            for proj in projets:
                if not isinstance(proj, dict):
                    continue

                projects.append({
                    "nom_projet": proj.get("nomProjet")
                })

    client.close()
    kwargs['ti'].xcom_push(key='mongo_projects', value=projects)
    logger.info(f"{len(projects)} projets extraits depuis MongoDB.")

def transform_data(conn_id, **kwargs):
    mongo_projects = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_projects')
    existing = get_existing_projects(conn_id)
    max_pk = get_max_project_id(conn_id)

    transformed = []

    for proj in mongo_projects:
        key = proj["nom_projet"]
        if key in existing:
            pk, code = existing[key]
        else:
            max_pk += 1
            pk = max_pk
            code = generate_project_code(pk)
            existing[key] = (pk, code)

        proj.update({
            "projet_pk": pk,
            "code_projet": code
        })
        transformed.append(proj)

    kwargs['ti'].xcom_push(key='transformed_projects', value=transformed)
    logger.info(f"{len(transformed)} projets transformés.")
    return transformed

def load_into_postgres(conn_id, **kwargs):
    projects = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_projects')
    if not projects:
        logger.info("Aucune donnée à charger.")
        return

    conn = get_postgresql_connection(conn_id)
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_projet (
        projet_id, code_projet, nom_projet
    ) VALUES (%s, %s, %s)
    ON CONFLICT (projet_id) DO UPDATE SET
        nom_projet = EXCLUDED.nom_projet,
        code_projet = EXCLUDED.code_projet;
    """

    for p in projects:
        cur.execute(insert_query, (
            p["projet_pk"],
            p["code_projet"],
            p["nom_projet"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(projects)} projets insérés ou mis à jour.")

dag = DAG(
    'dag_dim_projet',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

conn_id = 'postgres'

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[conn_id],
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    op_args=[conn_id],
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

start_task >> extract_task >> transform_task >> load_task >> end_task
