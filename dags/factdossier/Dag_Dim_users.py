import json
import logging
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return db["users"], db["privileges"]

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("PostgreSQL connection successful.")
    return conn

def get_next_users_pk():
    """
    Get the next available primary key for users by checking the maximum `user_pk`
    value from the PostgreSQL table.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(user_pk), 0) FROM public.dim_user;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk + 1  # Increment the max value by 1 to get the next available PK

def generate_codeusers(index):
    """
    Generate the user code (`codeuser`) based on the primary key `user_pk`.
    """
    return f"influ{index:04d}"

def extract_users(**kwargs):
    """
    Extract user data from MongoDB, generate primary keys, and prepare the data for insertion.
    """
    users_collection, privileges_collection = get_mongodb_collections()

    # Create a map for privilege labels to quickly access them by their ID
    privilege_map = {
        str(p["_id"]): p.get("label", "Non défini")
        for p in privileges_collection.find({}, {"_id": 1, "label": 1})
    }

    users = []
    index = get_next_users_pk()  # Get the next available user_pk for insertion

    cursor = users_collection.find({}, {
        "name": 1,
        "last_name": 1,
        "privilege": 1
    })

    for user in cursor:
        # Generate user code and extract information
        codeinflu = generate_codeusers(index)
        nom = user.get("name", "")
        prenom = user.get("last_name", "")
        privilege_id = str(user.get("privilege", ""))
        privilege_label = privilege_map.get(privilege_id, "Non défini")
        
        # Prepare the user data to insert into the database
        users.append((index, codeinflu, nom, prenom, privilege_label))
        index += 1  # Increment index for the next user

    kwargs['ti'].xcom_push(key='users_data', value=users)
    logger.info(f"{len(users)} utilisateurs extraits.")

def insert_users_to_dim_users(**kwargs):
    """
    Insert or update user data in PostgreSQL.
    """
    users = kwargs['ti'].xcom_pull(task_ids='extract_users', key='users_data')
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Define insert query with ON CONFLICT handling to avoid duplicates
    insert_query = """
        INSERT INTO dim_user (user_pk, codeuser, nom, prenom, privilege)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_pk) DO UPDATE SET
            codeuser = EXCLUDED.codeuser,
            nom = EXCLUDED.nom,
            prenom = EXCLUDED.prenom,
            privilege = EXCLUDED.privilege;
    """

    inserted_count = 0
    for user in users:
        cur.execute(insert_query, user)
        inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{inserted_count} utilisateurs insérés/mis à jour dans PostgreSQL.")

with DAG(
    dag_id='Dag_DimUser',
    schedule_interval='@daily',
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
        python_callable=insert_users_to_dim_users,
        provide_context=True
    )

    extract_task >> load_task
