import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
        collection = client["PowerBi"]["frontusers"]
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        raise

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_existing_projects_and_max_pk():
    conn = get_postgresql_connection()
    cur = conn.cursor()

    cur.execute("SELECT projet_id, nom_projet, nom_entreprise_projet, code_projet FROM dim_projet")
    existing = {(row[1], row[2]): (row[0], row[3]) for row in cur.fetchall()}

    cur.execute("SELECT COALESCE(MAX(projet_id), 0) FROM dim_projet")
    max_pk = cur.fetchone()[0]

    cur.close()
    conn.close()
    return existing, max_pk

def generate_project_code(pk):
    return f"PROJ{str(pk).zfill(3)}"

def safe_int(value):
    try:
        return int(value) if value else 0
    except:
        return 0

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
                date_debut = proj.get("dateDebut", {})
                date_fin = proj.get("dateFin", {})

                year_start = safe_int(date_debut.get("year"))
                month_start = safe_int(date_debut.get("month"))
                year_end = safe_int(date_fin.get("year"))
                month_end = safe_int(date_fin.get("month"))

                projects.append({
                    "nom_projet": proj.get("nomProjet"),
                    "entreprise": proj.get("entreprise"),
                    "year_start": year_start,
                    "month_start": month_start,
                    "year_end": year_end,
                    "month_end": month_end
                })

    client.close()
    kwargs['ti'].xcom_push(key='mongo_projects', value=projects)
    logger.info(f"{len(projects)} projets extraits depuis MongoDB.")

def transform_data(**kwargs):
    mongo_projects = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_projects')
    existing, max_pk = get_existing_projects_and_max_pk()

    transformed = []

    for proj in mongo_projects:
        key = (proj["nom_projet"], proj["entreprise"])
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

def load_into_postgres(**kwargs):
    projects = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_projects')
    if not projects:
        logger.info("Aucune donnée à charger.")
        return

    conn = get_postgresql_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_projet (
        projet_id, code_projet, nom_projet, annee_debut_projet, mois_debut_projet,
        annee_fin_projet, mois_fin_projet, nom_entreprise_projet
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (projet_id) DO UPDATE SET
        nom_projet = EXCLUDED.nom_projet,
        annee_debut_projet = EXCLUDED.annee_debut_projet,
        mois_debut_projet = EXCLUDED.mois_debut_projet,
        annee_fin_projet = EXCLUDED.annee_fin_projet,
        mois_fin_projet = EXCLUDED.mois_fin_projet,
        nom_entreprise_projet = EXCLUDED.nom_entreprise_projet,
        code_projet = EXCLUDED.code_projet;
    """

    for p in projects:
        cur.execute(insert_query, (
            p["projet_pk"],
            p["code_projet"],
            p["nom_projet"],
            p["year_start"],
            p["month_start"],
            p["year_end"],
            p["month_end"],
            p["entreprise"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(projects)} projets insérés ou mis à jour.")

dag = DAG(
    'dag_dim_projet',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
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
    task_id='load_into_postgres',
    python_callable=load_into_postgres,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task
