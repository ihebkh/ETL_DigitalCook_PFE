import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()



def get_mongodb_connection():
    MONGO_URI = Variable.get("MONGO_URI")
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    factures_collection = mongo_db["factures"]
    return client, mongo_db, factures_collection

def generate_service_pk():
    if not hasattr(generate_service_pk, "counter"):
        generate_service_pk.counter = 1
    pk = generate_service_pk.counter
    generate_service_pk.counter += 1
    return pk

def generate_service_code():
    service_pk = generate_service_pk()
    service_code = f"ser{str(service_pk).zfill(4)}"
    return service_code

def generate_service_with_pk():
    client, mongo_db, factures_collection = get_mongodb_connection()
    factures_cursor = factures_collection.find()
    service_details = []
    for facture in factures_cursor:
        for service in facture.get('services', []):
            service_pk = generate_service_pk()
            service_code = generate_service_code()
            service_name = service.get('nomService')
            service_details.append({
                'service_pk': service_pk,
                'service_code': service_code,
                'nomService': service_name
            })
    return service_details

def transform(service_details):
    seen_service_pks = set()
    unique_services = []
    for service in service_details:
        if service['service_pk'] not in seen_service_pks:
            unique_services.append(service)
            seen_service_pks.add(service['service_pk'])
    return unique_services

def check_service_exists(cursor, nom_service):
    cursor.execute("""SELECT 1 FROM public.dim_service WHERE nom_service = %s""", (nom_service,))
    return cursor.fetchone() is not None

def insert_or_update_postgres(service_details):
    conn = get_postgres_connection()
    cursor = conn.cursor()
    unique_service_details = transform(service_details)
    for service in unique_service_details:
        if not check_service_exists(cursor, service['nomService']):
            cursor.execute("""
                INSERT INTO public.dim_service (service_id, code_service, nom_service)
                VALUES (%s, %s, %s)
                ON CONFLICT (service_id)
                DO UPDATE SET 
                    code_service = EXCLUDED.code_service,
                    nom_service = EXCLUDED.nom_service;
            """, (service['service_pk'], service['service_code'], service['nomService']))
        else:
            logger.info(f"Le service avec le nom '{service['nomService']}' existe déjà, ignoré.")
    conn.commit()
    cursor.close()
    conn.close()

def extract_services(**kwargs):
    service_details = generate_service_with_pk()
    kwargs['ti'].xcom_push(key='service_details', value=service_details)
    logger.info(f"{len(service_details)} services extraits.")

def load_services_to_postgres(**kwargs):
    service_details = kwargs['ti'].xcom_pull(task_ids='extract_services', key='service_details')
    insert_or_update_postgres(service_details)
    logger.info(f"{len(service_details)} services insérés ou mis à jour dans PostgreSQL.")

dag = DAG(
    'dag_dim_service',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_services',
    python_callable=extract_services,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_services_to_postgres',
    python_callable=load_services_to_postgres,
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

start_task >> extract_task >> load_task >> end_task
