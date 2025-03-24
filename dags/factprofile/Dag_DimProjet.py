import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    """MongoDB connection."""
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        MONGO_COLLECTION = "frontusers"
        
        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        logger.info("MongoDB connection successful.")
        return client, mongo_db, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def get_postgresql_connection():
    """PostgreSQL connection."""
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def generate_project_code(existing_codes):
    """Generate next project code."""
    if not existing_codes:
        new_number = 1 
    else:
        last_numbers = [int(code[4:]) for code in existing_codes if code.startswith("PROJ") and code[4:].isdigit()]
        new_number = max(last_numbers) + 1 if last_numbers else 1
    return f"PROJ{str(new_number).zfill(2)}" 

def safe_int(value):
    """Convert to integer, return 0 if conversion fails or value is empty."""
    try:
        return int(value) if value else 0
    except ValueError:
        return 0

def extract_from_mongodb(**kwargs):
    """Extract data from MongoDB."""
    try:
        client, _, collection = get_mongodb_connection()
        mongo_data = collection.find({}, {"_id": 0, "profile.projets": 1, "simpleProfile.projets": 1})

        projects = []

        for user in mongo_data:
            if "profile" in user and "projets" in user["profile"]:
                for project in user["profile"]["projets"]:
                    if isinstance(project, dict):
                        date_debut = project.get("dateDebut", {})
                        date_fin = project.get("dateFin", {})
                    else:
                        logger.warning(f"Expected a dictionary for project but got: {type(project)}. Skipping this project.")
                        continue
                    
                    year_start = safe_int(date_debut.get("year", '0') if isinstance(date_debut, dict) else date_debut or '0')
                    month_start = safe_int(date_debut.get("month", '0') if isinstance(date_debut, dict) else '0')
                    year_end = safe_int(date_fin.get("year", '0') if isinstance(date_fin, dict) else date_fin or '0')
                    month_end = safe_int(date_fin.get("month", '0') if isinstance(date_fin, dict) else '0')

                    projects.append({
                        "nom_projet": project.get("nomProjet"),
                        "year_start": year_start,
                        "month_start": month_start,
                        "year_end": year_end,
                        "month_end": month_end,
                        "entreprise": project.get("entreprise"),
                        "code_projet": None 
                    })

            if "simpleProfile" in user and "projets" in user["simpleProfile"]:
                for project in user["simpleProfile"]["projets"]:
                    if isinstance(project, dict):
                        date_debut = project.get("dateDebut", {})
                        date_fin = project.get("dateFin", {})
                    else:
                        logger.warning(f"Expected a dictionary for project but got: {type(project)}. Skipping this project.")
                        continue
                    
                    year_start = safe_int(date_debut.get("year", '0') if isinstance(date_debut, dict) else date_debut or '0')
                    month_start = safe_int(date_debut.get("month", '0') if isinstance(date_debut, dict) else '0')
                    year_end = safe_int(date_fin.get("year", '0') if isinstance(date_fin, dict) else date_fin or '0')
                    month_end = safe_int(date_fin.get("month", '0') if isinstance(date_fin, dict) else '0')

                    projects.append({
                        "nom_projet": project.get("nomProjet"),
                        "year_start": year_start,
                        "month_start": month_start,
                        "year_end": year_end,
                        "month_end": month_end,
                        "entreprise": project.get("entreprise"),
                        "code_projet": None 
                    })

        client.close()
        kwargs['ti'].xcom_push(key='mongo_data', value=projects)
        logger.info(f"Extracted {len(projects)} projects from MongoDB.")
        return projects
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise


def transform_data(**kwargs):
    """Transform extracted data."""
    mongo_data = kwargs['ti'].xcom_pull(task_ids='extract_from_mongodb', key='mongo_data')
    transformed_projects = []

    conn = get_postgresql_connection()
    cur = conn.cursor()

    cur.execute("SELECT code_projet, nom_projet, entreprise FROM dim_projet")
    existing_projects = {f"{row[1]}_{row[2]}": row[0] for row in cur.fetchall()} 

    conn.close()

    for record in mongo_data:
        project_key = f"{record['nom_projet']}_{record['entreprise']}"
        
        if project_key in existing_projects:
            record["code_projet"] = existing_projects[project_key]
        else:
            record["code_projet"] = generate_project_code(existing_projects.values())
            existing_projects[project_key] = record["code_projet"]
        
        transformed_projects.append(record)

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_projects)
    return transformed_projects

def load_into_postgres(**kwargs):
    """Load transformed data into PostgreSQL."""
    try:
        transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

        if not transformed_data:
            logger.info("No data to insert into PostgreSQL.")
            return

        conn = get_postgresql_connection()
        cur = conn.cursor()

        insert_query = """
        INSERT INTO dim_projet (code_projet, nom_projet, year_start, month_start, year_end, month_end, entreprise)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (code_projet) DO UPDATE SET
            nom_projet = EXCLUDED.nom_projet,
            year_start = EXCLUDED.year_start,
            month_start = EXCLUDED.month_start,
            year_end = EXCLUDED.year_end,
            month_end = EXCLUDED.month_end,
            entreprise = EXCLUDED.entreprise
        """

        for record in transformed_data:
            values = (
                record["code_projet"],
                record["nom_projet"],
                record["year_start"],
                record["month_start"],
                record["year_end"],
                record["month_end"],
                record["entreprise"]
            )
            logger.info(f"Inserting / Updating: {values}")
            cur.execute(insert_query, values)

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{len(transformed_data)} project records inserted/updated in PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

dag = DAG(
    'Dag_DimProjet',
    schedule_interval='*/2 * * * *',
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
    task_id='transform_data',
    python_callable=transform_data,
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
