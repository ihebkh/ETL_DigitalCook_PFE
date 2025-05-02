import logging
from pymongo import MongoClient
from bson import ObjectId
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    logger.info("Connexion PostgreSQL réussie.")
    return conn.cursor(), conn

def get_secteur_map(cursor):
    cursor.execute("SELECT secteur_id, LOWER(nom_secteur) FROM public.dim_secteur;")
    return {label: pk for pk, label in cursor.fetchall()}

def get_metier_map(cursor):
    cursor.execute("SELECT metier_id, LOWER(nom_metier) FROM public.dim_metier;")
    return {label: pk for pk, label in cursor.fetchall()}

def get_entreprise_map(cursor):
    cursor.execute("SELECT entreprise_id, LOWER(nom_entreprise) FROM public.dim_entreprise;")
    return {label: pk for pk, label in cursor.fetchall()}

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["secteurdactivities"]

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def extract_offres_from_mongo():
    client, offres_col, _ = get_mongo_collections()
    cursor = offres_col.find({"isDeleted": False})
    offres = []

    for doc in cursor:
        offres.append({
            "_id": doc.get("_id"),
            "titre": doc.get("titre", "").strip(),
            "entreprise": doc.get("entreprise"),
            "secteur": doc.get("secteur"),
            "metier": doc.get("metier", []),
            "typeContrat": doc.get("typeContrat", "—"),
            "societe": doc.get("societe", "—"),
            "lieuSociete": doc.get("lieuSociete", "—"),
            "pays": doc.get("pays", "—"),
        })

    client.close()
    return convert_bson(offres)

def transform_offres(raw_offres, cursor):
    secteur_map = get_secteur_map(cursor)
    metier_map = get_metier_map(cursor)
    entreprise_map = get_entreprise_map(cursor)

    seen_titles = set()
    counter = 1
    transformed = []

    for doc in raw_offres:
        titre = doc["titre"]
        if not titre or titre.lower() in seen_titles:
            continue
        seen_titles.add(titre.lower())
        offre_code = f"OFFR{str(counter).zfill(4)}"

        secteur_fk = metier_fk = entreprise_fk = None
        entreprise_fk = entreprise_map.get(doc["societe"].strip().lower())

        secteur_id = doc.get("secteur")
        metier_ids = doc.get("metier", [])
        if not isinstance(metier_ids, list):
            metier_ids = [metier_ids]

        if secteur_id and ObjectId.is_valid(secteur_id):
            client, _, secteurs_col = get_mongo_collections()
            secteur_doc = secteurs_col.find_one({"_id": ObjectId(secteur_id)})
            if secteur_doc:
                label = secteur_doc.get("label", "").strip().lower()
                secteur_fk = secteur_map.get(label)
                for job in secteur_doc.get("jobs", []):
                    if str(job.get("_id")) in metier_ids:
                        metier_label = job.get("label", "").strip().lower()
                        metier_fk = metier_map.get(metier_label)
                        break

        transformed.append({
            "offre_code": offre_code,
            "titre": titre,
            "secteur_fk": secteur_fk,
            "metier_fk": metier_fk,
            "entreprise_fk": entreprise_fk,
            "typeContrat": doc["typeContrat"],
            "pays": doc["pays"],
        })
        counter += 1

    return transformed

def load_offres_to_postgres(transformed_offres):
    cursor, conn = get_postgres_connection()

    for offre in transformed_offres:
        record = (
            offre.get('offre_code'),
            offre.get('titre'),
            offre.get('secteur_fk', None),
            offre.get('metier_fk', None),
            offre.get('entreprise_fk', None),
            offre.get('typeContrat'),
            offre.get('pays')
        )

        cursor.execute("""
            INSERT INTO public.dim_offre_emploi (
                code_offre_emploi, titre_offre_emploi, secteur_id, metier_id, entreprise_id,
                type_contrat_emploi, pays_emploi
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code_offre_emploi) DO UPDATE SET
                titre_offre_emploi = EXCLUDED.titre_offre_emploi,
                secteur_id = EXCLUDED.secteur_id,
                metier_id = EXCLUDED.metier_id,
                entreprise_id = EXCLUDED.entreprise_id,
                type_contrat_emploi = EXCLUDED.type_contrat_emploi,
                pays_emploi = EXCLUDED.pays_emploi;
        """, record)

    conn.commit()
    cursor.close()
    conn.close()

dag = DAG(
    dag_id='dag_dim_offre_emplois',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

def extract_task():
    return extract_offres_from_mongo()

def transform_task(**kwargs):
    raw_offres = kwargs['ti'].xcom_pull(task_ids='extract_task')
    cursor, _ = get_postgres_connection()
    return transform_offres(raw_offres, cursor)

def load_task(**kwargs):
    transformed_offres = kwargs['ti'].xcom_pull(task_ids='transform_task')
    load_offres_to_postgres(transformed_offres)

extract = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)
wait_dim_secteur = ExternalTaskSensor(
    task_id='wait_for_dim_secteur',
    external_dag_id='dag_dim_secteur',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_entreprise = ExternalTaskSensor(
    task_id='wait_for_dim_entreprise',
    external_dag_id='dag_dim_entreprise',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag 
)


wait_dim_metier = ExternalTaskSensor(
    task_id='wait_for_dim_metier',
    external_dag_id='Dag_Metier',
    external_task_id='load_jobs_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

[wait_dim_metier,wait_dim_entreprise,wait_dim_secteur]>>extract >> transform >> load
