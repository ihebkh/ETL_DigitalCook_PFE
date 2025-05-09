from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pymongo import MongoClient
from bson import ObjectId
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_code_offre(index: int) -> str:
    return f"OFFRE{index:04d}"

def generate_offre_etude_pk(index: int) -> int:
    return index

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredetudes"], db["universities"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info(" Connexion PostgreSQL réussie via PostgresHook.")
    return conn


def load_universite_mapping(pg_conn):
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT universite_id, nom_universite FROM public.dim_universite;")
        rows = cursor.fetchall()
        return {row[1].strip().lower(): row[0] for row in rows}


def extract_offres_from_mongo(**context):
    mongo_client, offers_collection, universities_collection = get_mongo_collections()
    pg_conn = get_postgres_connection()

    try:
        universite_map = load_universite_mapping(pg_conn)
        offers = offers_collection.find({}, {"titre": 1, "university": 1})
        extracted_rows = []

        for index, offer in enumerate(offers, start=1):
            titre = offer.get("titre", "Sans titre")
            university_id = offer.get("university")
            universite_pk = None

            if isinstance(university_id, ObjectId):
                university_doc = universities_collection.find_one({"_id": university_id})
                if university_doc:
                    nom_univ = university_doc.get("nom", "").strip().lower()
                    universite_pk = universite_map.get(nom_univ)

            if universite_pk is not None:
                offre_etude_pk = generate_offre_etude_pk(index)
                code_offre = generate_code_offre(index)

                extracted_rows.append((
                    offre_etude_pk,
                    code_offre,
                    titre,
                    universite_pk
                ))

        context['ti'].xcom_push(key='offres_data', value=extracted_rows)
        logger.info(f" {len(extracted_rows)} offres extraites depuis MongoDB.")
    finally:
        pg_conn.close()
        mongo_client.close()


def insert_offres_into_postgres(**context):
    offres_data = context['ti'].xcom_pull(task_ids='extract_offres_task', key='offres_data')
    if not offres_data:
        logger.warning(" Aucune donnée à insérer.")
        return

    pg_conn = get_postgres_connection()
    try:
        with pg_conn.cursor() as cursor:
            insert_query = """
                INSERT INTO public.dim_offre_etude (
                    offre_etude_id, code_offre_etude, titre_offre_etude, universite_id
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (titre_offre_etude) DO UPDATE SET
                    titre_offre_etude = EXCLUDED.titre_offre_etude,
                    universite_id = EXCLUDED.universite_id;
            """
            cursor.executemany(insert_query, offres_data)
            pg_conn.commit()
            logger.info(f" {len(offres_data)} offres insérées ou mises à jour dans PostgreSQL.")
    finally:
        pg_conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dag_dim_offre_etude',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    start = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting formation extraction process..."),
    dag=dag
)

    extract_task = PythonOperator(
        task_id='extract_offres_task',
        python_callable=extract_offres_from_mongo,
        provide_context=True
    )

    insert_task = PythonOperator(
        task_id='insert_offres_task',
        python_callable=insert_offres_into_postgres,
        provide_context=True
    )
    wait_dim_universite = ExternalTaskSensor(
    task_id='wait_for_dim_universite',
    external_dag_id='dag_dim_universite',
    external_task_id='load_dim_universite',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)
    end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Formation extraction process completed."),
    dag=dag
)

start>>wait_dim_universite >> extract_task >> insert_task>>end_task
