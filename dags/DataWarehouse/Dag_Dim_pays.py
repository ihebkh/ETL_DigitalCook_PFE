import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
from contextlib import contextmanager
from datetime import datetime
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@contextmanager
def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    try:
        yield conn
    finally:
        conn.close()

def get_mongodb_collections():
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["PowerBi"]
    return client, db["pays"]

def extract_from_mongodb(**kwargs):
    try:
        client, collection = get_mongodb_collections()
        documents = list(collection.find({}, {"_id": 0, "country": 1}))
        pays_list = [(doc["country"]) for doc in documents if "country" in doc]
        kwargs['ti'].xcom_push(key='pays_data', value=pays_list)
        logger.info(f"{len(pays_list)} noms de pays extraits depuis MongoDB.")
        client.close()
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction MongoDB : {e}")
        raise

def transform_pays(**kwargs):
    pays_data = kwargs['ti'].xcom_pull(task_ids='extract_pays', key='pays_data')
    if not pays_data:
        logger.warning("Aucun pays à transformer.")
        return []

    transformed_pays = []
    for pays in pays_data:
        pays_clean = pays.strip().title()  
        transformed_pays.append(pays_clean)

    logger.info(f"{len(transformed_pays)} pays transformés.")
    kwargs['ti'].xcom_push(key='transformed_pays_data', value=transformed_pays)
    return transformed_pays

def insert_pays_to_postgres(**kwargs):
    try:
        pays = kwargs['ti'].xcom_pull(task_ids='transform_pays_task', key='transformed_pays_data')
        if not pays:
            logger.info("Aucun pays à insérer.")
            return

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO dim_pays (code_pays, nom_pays)
                VALUES (%s, %s)
                ON CONFLICT (nom_pays) DO UPDATE
                SET nom_pays = EXCLUDED.nom_pays;
                """

                for index, nom in enumerate(pays, start=1):
                    code = f"PAY{index:04d}"
                    cur.execute(insert_query, (code, nom))

                conn.commit()

        logger.info(f"{len(pays)} pays insérés/mis à jour.")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des pays : {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='Dag_dim_pays',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info(" Début du DAG d'insertion/mise à jour des pays."),
        dag=dag
    )

    extract_task = PythonOperator(
        task_id='extract_pays',
        python_callable=extract_from_mongodb,
        provide_context=True,
        dag=dag
    )

    transform_task = PythonOperator(
        task_id='transform_pays_task',
        python_callable=transform_pays,
        provide_context=True,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='insert_pays_to_postgres',
        python_callable=insert_pays_to_postgres,
        provide_context=True,
        dag=dag
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info(" Fin du DAG d'insertion/mise à jour des pays."),
        dag=dag
    )

    start_task >> extract_task >> transform_task >> load_task >> end_task
