import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongodb_collections():
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["PowerBi"]
    return db["users"], db["privileges"]

def get_existing_user_keys(cursor):
    cursor.execute("SELECT nom_recruteur, prenom_recruteur, privilege_recruteur FROM dim_recruteur;")
    return {(row[0], row[1], row[2]) for row in cursor.fetchall()}

def get_next_users_pk(cursor):
    cursor.execute("SELECT COALESCE(MAX(recruteur_id), 0) FROM public.dim_recruteur;")
    return cursor.fetchone()[0] + 1

def generate_codeusers(index):
    return f"influ{index:04d}"


def extract_users(**kwargs):
    try:
        users_collection, privileges_collection = get_mongodb_collections()
        conn = get_postgres_connection()
        cursor = conn.cursor()

        existing_keys = get_existing_user_keys(cursor)
        index = get_next_users_pk(cursor)

        privilege_map = {
            str(p["_id"]): p.get("label", "Non défini")
            for p in privileges_collection.find({}, {"_id": 1, "label": 1})
        }

        users = []
        cursor_mongo = users_collection.find({}, {
            "name": 1,
            "last_name": 1,
            "privilege": 1
        })

        for user in cursor_mongo:
            nom = user.get("name", "")
            prenom = user.get("last_name", "")
            privilege_id = str(user.get("privilege", ""))
            privilege_label = privilege_map.get(privilege_id, "Non défini")

            if (nom, prenom, privilege_label) in existing_keys:
                continue

            codeinflu = generate_codeusers(index)
            users.append((index, codeinflu, nom, prenom, privilege_label))
            index += 1

        cursor.close()
        conn.close()

        kwargs['ti'].xcom_push(key='users_data', value=users)
        logger.info(f"{len(users)} nouveaux utilisateurs extraits sans doublon.")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des utilisateurs: {str(e)}")
        raise


def transform_users(**kwargs):
    users_data = kwargs['ti'].xcom_pull(task_ids='extract_users', key='users_data')
    if not users_data:
        logger.warning("Aucune donnée à transformer.")
        return []

    transformed_users = []

    for user in users_data:
        index, codeinflu, nom, prenom, privilege_label = user

        nom = nom.strip()
        prenom = prenom.strip()
        privilege_label = privilege_label.strip()

        nom = nom.upper()
        prenom = prenom.upper()

        transformed_users.append((index, codeinflu, nom, prenom, privilege_label))

    logger.info(f"{len(transformed_users)} utilisateurs transformés.")
    kwargs['ti'].xcom_push(key='transformed_users_data', value=transformed_users)
    return transformed_users


def insert_users_to_dim_users(**kwargs):
    users_data = kwargs['ti'].xcom_pull(task_ids='transform_users_task', key='transformed_users_data')
    if not users_data:
        logger.warning("Aucun utilisateur transformé à insérer.")
        return

    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_recruteur (
                    recruteur_id, code_recruteur, nom_recruteur, prenom_recruteur, privilege_recruteur
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (recruteur_id) DO UPDATE SET
                    code_recruteur = EXCLUDED.code_recruteur,
                    nom_recruteur = EXCLUDED.nom_recruteur,
                    prenom_recruteur = EXCLUDED.prenom_recruteur,
                    privilege_recruteur = EXCLUDED.privilege_recruteur;
            """
            cursor.executemany(insert_query, users_data)
            conn.commit()
            logger.info(f"{len(users_data)} utilisateurs insérés ou mis à jour dans PostgreSQL.")
    finally:
        conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='Dag_dim_recruteur',
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("Démarrage du processus d'extraction des recruteurs..."),
        dag=dag
    )

    extract_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
        provide_context=True,
        dag=dag
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_users,
        provide_context=True,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert_users_to_dim_users,
        provide_context=True,
        dag=dag
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Processus d'extraction des recruteurs terminé."),
        dag=dag
    )

    start_task >> extract_task >> transform_task >> load_task >> end_task
