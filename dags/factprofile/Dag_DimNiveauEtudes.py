import logging
import json
from datetime import datetime
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    return client["PowerBi"]["frontusers"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def parse_date(date_input):
    if isinstance(date_input, str):
        try:
            date = datetime.strptime(date_input, "%Y-%m-%d")
            return date.month, date.year
        except:
            try:
                date = datetime.strptime(date_input, "%Y")
                return None, date.year
            except:
                return None, None
    elif isinstance(date_input, dict):
        return date_input.get("month"), date_input.get("year")
    return None, None

def extract_niveau_etudes(**kwargs):
    collection = get_mongodb_connection()
    data = []
    compteur = 1

    for doc in collection.find():
        for etude in doc.get("simpleProfile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            code = f"DIP{compteur:03d}"
            compteur += 1
            universite = etude.get("school", "null")
            course = etude.get("course", None)
            label = etude.get("label", "null")
            print(label)
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")
            du = etude.get("duree", {}).get("du", {})
            au = etude.get("duree", {}).get("au", {})
            start_m, start_y = parse_date(du)
            end_m, end_y = parse_date(au)
            data.append({"code": code, "label": label, "universite": universite,
                         "start_year": start_y if start_y not in ["", "null"] else None,
                         "start_month": start_m if start_m not in ["", "null"] else None,
                         "end_year": end_y if end_y not in ["", "null"] else None,
                         "end_month": end_m if end_m not in ["", "null"] else None,
                         "diplome": diplome, "pays": pays, "course": course})

        for etude in doc.get("profile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            code = f"DIP{compteur:03d}"
            compteur += 1
            universite = etude.get("universite", "null")
            course = None
            label = etude.get("label", "null")
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")
            start_m, start_y = parse_date(etude.get("du", {}))
            end_m, end_y = parse_date(etude.get("au", {}))
            data.append({"code": code, "label": label, "universite": universite,
                         "start_year": start_y if start_y not in ["", "null"] else None,
                         "start_month": start_m if start_m not in ["", "null"] else None,
                         "end_year": end_y if end_y not in ["", "null"] else None,
                         "end_month": end_m if end_m not in ["", "null"] else None,
                         "diplome": diplome, "pays": pays, "course": course})

    kwargs['ti'].xcom_push(key='dim_niveaux', value=data)
    logger.info(f"{len(data)} niveaux d’études extraits.")

def load_niveau_etudes_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_dim_niveau_etudes', key='dim_niveaux')
    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_niveau_d_etudes (
        diplome_code, label, universite,
        start_year, start_month, end_year, end_month,
        nom_diplome, pays, course
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (diplome_code)
    DO UPDATE SET
        start_year = EXCLUDED.start_year,
        start_month = EXCLUDED.start_month,
        end_year = EXCLUDED.end_year,
        end_month = EXCLUDED.end_month,
        pays = EXCLUDED.pays,
        course = EXCLUDED.course
    """

    for row in data:
        cur.execute(insert_query, (
            row['code'], row['label'], row['universite'],
            row['start_year'], row['start_month'],
            row['end_year'], row['end_month'],
            row['diplome'], row['pays'], row['course']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} niveaux d’études insérés ou mis à jour.")

# DAG
with DAG(
    dag_id='dag_dim_niveau_etudes',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_dim_niveau_etudes',
        python_callable=extract_niveau_etudes,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_dim_niveau_etudes',
        python_callable=load_niveau_etudes_postgres,
        provide_context=True,
    )

    extract_task >> load_task
