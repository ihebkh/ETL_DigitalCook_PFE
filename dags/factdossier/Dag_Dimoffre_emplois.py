import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["entreprises"], db["secteurdactivities"]

def convert_bson(obj):
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def extract_offres_from_mongo(**kwargs):
    try:
        client, offres_col, _, _ = get_mongo_collections()
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
                "tempsDeTravail": doc.get("tempsDeTravail", "—"),
                "societe": doc.get("societe", "—"),
                "lieuSociete": doc.get("lieuSociete", "—"),
                "deviseSalaire": doc.get("deviseSalaire", "—"),
                "salaireBrutPar": doc.get("salaireBrutPar", "—"),
                "niveauDexperience": doc.get("niveauDexperience", "—"),
                "disponibilite": doc.get("disponibilite", "—"),
                "pays": doc.get("pays", "—"),
                "onSiteOrRemote": doc.get("onSiteOrRemote", "—")
            })

        client.close()
        cleaned = convert_bson(offres)
        logger.info(f" {len(cleaned)} documents extraits depuis MongoDB.")
        kwargs['ti'].xcom_push(key='raw_offres', value=cleaned)

    except Exception as e:
        logger.error(f" Erreur durant l'extraction MongoDB : {e}")
        raise

def get_secteur_map(cur):
    cur.execute("SELECT secteur_pk, LOWER(label) FROM public.dim_secteur;")
    return {label: pk for pk, label in cur.fetchall()}

def get_metier_map(cur):
    cur.execute("SELECT metier_pk, LOWER(label_jobs) FROM public.dim_metier;")
    return {label: pk for pk, label in cur.fetchall()}

def get_entreprise_map(cur):
    cur.execute("SELECT entreprise_pk, LOWER(nom) FROM public.dim_entreprise;")
    return {label: pk for pk, label in cur.fetchall()}

def transform_offres(**kwargs):
    try:
        raw_offres = kwargs['ti'].xcom_pull(task_ids='extract_offres_from_mongo', key='raw_offres')
        conn = get_postgres_connection()
        cur = conn.cursor()
        client, _, _, secteurs_col = get_mongo_collections()

        secteur_map = get_secteur_map(cur)
        metier_map = get_metier_map(cur)
        entreprise_map = get_entreprise_map(cur)

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
                secteur_doc = secteurs_col.find_one({"_id": ObjectId(secteur_id)})
                if secteur_doc:
                    label = secteur_doc.get("label", "").strip().lower()
                    secteur_fk = secteur_map.get(label)
                    for job in secteur_doc.get("jobs", []):
                        if str(job.get("_id")) in metier_ids:
                            metier_label = job.get("label", "").strip().lower()
                            metier_fk = metier_map.get(metier_label)
                            break

            transformed.append((
                counter,
                offre_code,
                titre,
                secteur_fk,
                metier_fk,
                entreprise_fk,
                doc["typeContrat"],
                doc["tempsDeTravail"],
                doc["deviseSalaire"],
                doc["salaireBrutPar"],
                doc["niveauDexperience"],
                doc["disponibilite"],
                doc["pays"],
                doc["onSiteOrRemote"]
            ))
            counter += 1

        client.close()
        cur.close()
        conn.close()

        logger.info(f" {len(transformed)} offres transformées avec succès.")
        kwargs['ti'].xcom_push(key='offres_transformed', value=transformed)

    except Exception as e:
        logger.error(f" Erreur durant la transformation : {e}")
        raise

def load_offres_to_postgres(**kwargs):
    try:
        offres = kwargs['ti'].xcom_pull(task_ids='transform_offres', key='offres_transformed')
        conn = get_postgres_connection()
        cur = conn.cursor()

        for record in offres:
            logger.info(f"Record : {record}")
            record = tuple(
                (r if r is not None and r != '' else None) for r in record
            )

            logger.info(f"Record modifié : {record}")

            cur.execute("""
                INSERT INTO public.dim_offreemploi (
                    offre_pk, offre_code, titre, secteur_fk, metier_fk, entreprise_fk,
                    type_contrat, temps_de_travail,
                    devise_salaire, salaire_brut_par, niveau_experience, disponibilite,
                    pays, on_site_or_remote
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (offre_pk) DO UPDATE SET
                    titre = EXCLUDED.titre,
                    secteur_fk = EXCLUDED.secteur_fk,
                    metier_fk = EXCLUDED.metier_fk,
                    entreprise_fk = EXCLUDED.entreprise_fk,
                    type_contrat = EXCLUDED.type_contrat,
                    temps_de_travail = EXCLUDED.temps_de_travail,
                    devise_salaire = EXCLUDED.devise_salaire,
                    salaire_brut_par = EXCLUDED.salaire_brut_par,
                    niveau_experience = EXCLUDED.niveau_experience,
                    disponibilite = EXCLUDED.disponibilite,
                    pays = EXCLUDED.pays,
                    on_site_or_remote = EXCLUDED.on_site_or_remote;
            """, record)

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f" {len(offres)} offres insérées ou mises à jour dans PostgreSQL.")

    except Exception as e:
        logger.error(f" Erreur lors du chargement dans PostgreSQL : {e}")
        raise

dag = DAG(
    dag_id='dag_dim_offreemploi',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

extract = PythonOperator(
    task_id='extract_offres_from_mongo',
    python_callable=extract_offres_from_mongo,
    provide_context=True,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_offres',
    python_callable=transform_offres,
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id='load_offres_to_postgres',
    python_callable=load_offres_to_postgres,
    provide_context=True,
    dag=dag
)

extract >> transform >> load
