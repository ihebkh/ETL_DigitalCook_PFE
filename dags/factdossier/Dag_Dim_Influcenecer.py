from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return db["users"], db["privileges"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def generate_codeinfluencer(index):
    return f"influ{index:04d}"

def extract_users(**kwargs):
    users_collection, privileges_collection = get_mongodb_collections()

    privilege_map = {
        str(p["_id"]): p.get("label", "Non défini")
        for p in privileges_collection.find({}, {"_id": 1, "label": 1})
    }

    users = []
    cursor = users_collection.find({}, {
        "name": 1,
        "last_name": 1,
        "is_admin": 1,
        "privilege": 1
    })

    index = 1
    for user in cursor:
        codeinflu = generate_codeinfluencer(index)
        nom = user.get("name", "")
        prenom = user.get("last_name", "")
        is_admin = user.get("is_admin", False)
        privilege_id = str(user.get("privilege", ""))
        privilege_label = privilege_map.get(privilege_id, "Non défini")
        users.append((codeinflu, nom, prenom, is_admin, privilege_label))
        index += 1

    kwargs['ti'].xcom_push(key='users_data', value=users)
    logger.info(f"{len(users)} utilisateurs extraits.")

def insert_users_to_dim_influencer(**kwargs):
    users = kwargs['ti'].xcom_pull(task_ids='extract_users', key='users_data')
    conn = get_postgres_connection()
    cur = conn.cursor()

    for user in users:
        cur.execute("""
            INSERT INTO dim_influencer (codeinfluencer, nom, prenom, is_admin, privilege)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (codeinfluencer) DO UPDATE SET
                nom = EXCLUDED.nom,
                prenom = EXCLUDED.prenom,
                is_admin = EXCLUDED.is_admin,
                privilege = EXCLUDED.privilege;
        """, user)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(users)} influenceurs insérés/mis à jour dans PostgreSQL.")

with DAG(
    dag_id='Dag_DimInfluencer',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_users_to_postgres',
        python_callable=insert_users_to_dim_influencer,
        provide_context=True
    )

    extract_task >> load_task
