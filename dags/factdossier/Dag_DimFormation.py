from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from pymongo import MongoClient
from bson import ObjectId
import datetime
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id="postgres")
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["formations"]
    return client, mongo_db, collection

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return obj

def generate_formation_code(index):
    return f"formation{str(index).zfill(4)}"

def get_next_formation_pk():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(formation_id) FROM public.dim_formation;")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return (max_pk or 0) + 1

def convert_to_datetime(date_value):
    if isinstance(date_value, datetime.datetime):
        return date_value
    elif isinstance(date_value, str):
        try:
            return datetime.datetime.fromisoformat(date_value)
        except ValueError:
            return None
    return None

def extract_formations(**kwargs):
    client, mongo_db, collection = get_mongodb_connection()
    formations = list(collection.find({}, {
        "_id": 0, "titreFormation": 1, "dateDebut": 1, "dateFin": 1, 
        "domaine": 1, "ville": 1, "centreDeFormation": 1, 
        "presence": 1, "duree": 1
    }))
    client.close()
    formations = convert_bson(formations)
    kwargs['ti'].xcom_push(key='formations', value=formations)
    logger.info(f"{len(formations)} formations extraites de MongoDB.")

def insert_if_not_exists(cursor, query_insert, data):
    formation_pk = data[0]
    cursor.execute("SELECT 1 FROM dim_formation WHERE formation_id = %s LIMIT 1;", (formation_pk,))
    if cursor.fetchone():
        logger.info(f"Duplicate found for formation_id: {formation_pk}. Skipping insert.")
    else:
        cursor.execute(query_insert, data)
        logger.info(f"Inserted new formation with formation_id: {formation_pk}.")

def load_formations(**kwargs):
    formations = kwargs['ti'].xcom_pull(task_ids='extract_formations', key='formations')
    if not formations:
        logger.info("Aucune formation à charger.")
        return

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query_insert = """
    INSERT INTO dim_formation (
        formation_id , code_formation , titre_formation, date_debut_formation, date_fin_formation, 
        domaine_formation, ville_formation, centre_formation, presence_formation, duree_formation
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (formation_id) DO UPDATE
    SET code_formation  = EXCLUDED.code_formation ,
        titre_formation = EXCLUDED.titre_formation,
        date_debut_formation = EXCLUDED.date_debut_formation,
        date_fin_formation = EXCLUDED.date_fin_formation,
        domaine_formation = EXCLUDED.domaine_formation,
        ville_formation = EXCLUDED.ville_formation,
        centre_formation = EXCLUDED.centre_formation,
        presence_formation = EXCLUDED.presence_formation,
        duree_formation = EXCLUDED.duree_formation;
    """

    pk_counter = get_next_formation_pk()

    for formation in formations:
        formation_pk = pk_counter
        code = generate_formation_code(pk_counter)
        titre = formation.get("titreFormation", "")
        date_debut = convert_to_datetime(formation.get("dateDebut"))
        date_fin = convert_to_datetime(formation.get("dateFin"))
        domaine = formation.get("domaine", "")
        ville = formation.get("ville", "")
        centre = formation.get("centreDeFormation", "")
        presence = formation.get("presence", "")
        duree = formation.get("duree", "")

        data = (
            formation_pk, code, titre, date_debut, date_fin,
            domaine, ville, centre, presence, duree
        )

        insert_if_not_exists(cursor, query_insert, data)

        pk_counter += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"{len(formations)} formations insérées, doublons ignorés.")

dag = DAG(
    dag_id='dag_dim_formation',
    start_date=dt(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_formations',
    python_callable=extract_formations,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_formations',
    python_callable=load_formations,
    provide_context=True,
    dag=dag
)

extract_task >> load_task
