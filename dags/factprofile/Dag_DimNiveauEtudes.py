import logging
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

def get_max_niveau_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(niveau_etude_id), 0) FROM dim_niveau_d_etudes")
    max_pk = cur.fetchone()[0]
    cur.close()
    conn.close()
    return max_pk

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

    for doc in collection.find():
        for etude in doc.get("simpleProfile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            universite = etude.get("school", "null")
            course = etude.get("course", None)
            label = etude.get("label", "null")
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")
            du = etude.get("duree", {}).get("du", {})
            au = etude.get("duree", {}).get("au", {})
            start_m, start_y = parse_date(du)
            end_m, end_y = parse_date(au)
            data.append({"label": label, "universite": universite,
                         "start_year": start_y, "start_month": start_m,
                         "end_year": end_y, "end_month": end_m,
                         "diplome": diplome, "pays": pays})

        for etude in doc.get("profile", {}).get("niveauDetudes", []):
            if not isinstance(etude, dict):
                continue
            universite = etude.get("universite", "null")
            label = etude.get("label", "null")
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")
            start_m, start_y = parse_date(etude.get("du", {}))
            end_m, end_y = parse_date(etude.get("au", {}))
            data.append({"label": label, "universite": universite,
                         "start_year": start_y, "start_month": start_m,
                         "end_year": end_y, "end_month": end_m,
                         "diplome": diplome, "pays": pays})

    kwargs['ti'].xcom_push(key='niveau_raw_data', value=data)
    logger.info(f"{len(data)} niveaux d’études extraits.")

def transform_niveau_etudes(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_dim_niveau_etudes', key='niveau_raw_data')
    if not raw_data:
        logger.info("Aucune donnée à transformer.")
        return

    max_pk = get_max_niveau_pk()
    transformed = []
    compteur = 0

    for row in raw_data:
        compteur += 1
        max_pk += 1
        code = f"DIP{compteur:03d}"

        if row["label"] in [None, "null", ""]:
            row["label"] = row.get("course", None)
            row["course"] = None 

        transformed.append({
            "niveau_pk": max_pk,
            "code": code,
            "label": row["label"],
            "universite": row["universite"],
            "start_year": row["start_year"],
            "start_month": row["start_month"],
            "end_year": row["end_year"],
            "end_month": row["end_month"],
            "pays": row["pays"]
        })

    kwargs['ti'].xcom_push(key='niveau_transformed', value=transformed)
    logger.info(f"{len(transformed)} lignes transformées avec succès.")

def load_niveau_etudes_postgres(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_niveau_etudes', key='niveau_transformed')
    if not data:
        logger.info("Aucune donnée à insérer.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_niveau_d_etudes (
        niveau_etude_id, code_diplome, nom_diplome, universite_etudes,
        annee_debut_etudes, mois_debut_etudes, annee_fin_etudes, mois_fin_etudes,
        pays_etudes
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (niveau_etude_id)
    DO UPDATE SET
        code_diplome = EXCLUDED.code_diplome,
        nom_diplome = EXCLUDED.nom_diplome,
        universite_etudes = EXCLUDED.universite_etudes,
        annee_debut_etudes = EXCLUDED.annee_debut_etudes,
        mois_debut_etudes = EXCLUDED.mois_debut_etudes,
        annee_fin_etudes = EXCLUDED.annee_fin_etudes,
        mois_fin_etudes = EXCLUDED.mois_fin_etudes,
        pays_etudes = EXCLUDED.pays_etudes
    """

    for row in data:
        cur.execute(insert_query, (
            row['niveau_pk'],
            row['code'],
            row['label'],
            row['universite'],
            int(row['start_year']) if str(row['start_year']).isdigit() else None,
            int(row['start_month']) if str(row['start_month']).isdigit() else None,
            int(row['end_year']) if str(row['end_year']).isdigit() else None,
            int(row['end_month']) if str(row['end_month']).isdigit() else None,
            row['pays']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"{len(data)} niveaux d’études insérés ou mis à jour.")

with DAG(
    dag_id='dag_dim_niveau_etudes',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_dim_niveau_etudes',
        python_callable=extract_niveau_etudes,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_niveau_etudes',
        python_callable=transform_niveau_etudes,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_dim_niveau_etudes',
        python_callable=load_niveau_etudes_postgres,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
