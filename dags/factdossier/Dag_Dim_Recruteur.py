import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import contextmanager

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
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return db["users"], db["privileges"]

def get_existing_user_keys():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nom_recruteur, prenom_recruteur, privilege_recruteur FROM dim_recruteur;")
            existing_keys = {(row[0], row[1], row[2]) for row in cur.fetchall()}
            return existing_keys

def get_next_users_pk():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(recruteur_id), 0) FROM public.dim_recruteur;")
            max_pk = cur.fetchone()[0]
            return max_pk + 1

def generate_codeusers(index):
    return f"influ{index:04d}"

def extract_users(**kwargs):
    try:
        users_collection, privileges_collection = get_mongodb_collections()
        privilege_map = {
            str(p["_id"]): p.get("label", "Non défini")
            for p in privileges_collection.find({}, {"_id": 1, "label": 1})
        }

        existing_keys = get_existing_user_keys()
        index = get_next_users_pk()

        users = []
        cursor = users_collection.find({}, {
            "name": 1,
            "last_name": 1,
            "privilege": 1
        })

        for user in cursor:
            nom = user.get("name", "")
            prenom = user.get("last_name", "")
            privilege_id = str(user.get("privilege", ""))
            privilege_label = privilege_map.get(privilege_id, "Non défini")

            if (nom, prenom, privilege_label) in existing_keys:
                continue

            codeinflu = generate_codeusers(index)
            users.append((index, codeinflu, nom, prenom, privilege_label))
            index += 1

        kwargs['ti'].xcom_push(key='users_data', value=users)
        logger.info(f"{len(users)} nouveaux utilisateurs extraits sans doublon.")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des utilisateurs: {str(e)}")
        raise

def insert_users_to_dim_users(**kwargs):
    try:
        users = kwargs['ti'].xcom_pull(task_ids='extract_users', key='users_data')
        if not users:
            logger.info("Aucun nouvel utilisateur à insérer.")
            return

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
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

                inserted_count = 0
                for user in users:
                    cur.execute(insert_query, user)
                    inserted_count += 1

                conn.commit()
                logger.info(f"{inserted_count} utilisateurs insérés/mis à jour dans PostgreSQL.")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des utilisateurs: {str(e)}")
        raise

with DAG(
    dag_id='Dag_dim_recruteur',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("Démarrage du processus d'extraction des recruteurs...")
    )

    extract_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_users_to_postgres',
        python_callable=insert_users_to_dim_users,
        provide_context=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("Processus d'extraction des recruteurs terminé.")
    )

    start_task >> extract_task >> load_task >> end_task