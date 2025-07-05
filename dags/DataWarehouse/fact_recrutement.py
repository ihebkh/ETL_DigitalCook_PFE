from pymongo import MongoClient
from datetime import datetime, timedelta
from bson import ObjectId
import logging
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from typing import Tuple, Optional, Dict, Any, List
from urllib.parse import quote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_cursor():
    conn = PostgresHook(postgres_conn_id='postgres').get_conn()
    conn.autocommit = False
    return conn.cursor(), conn

def get_client_pk_by_matricule(cursor, matricule):
    cursor.execute("SELECT client_id FROM public.dim_client WHERE matricule_client = %s", (str(matricule),))
    result = cursor.fetchone()
    return result[0] if result else None

def get_next_dossier_pk(pg_cursor):
    pg_cursor.execute('SELECT COALESCE(MAX(dossier_id), 0) FROM public."fact_recrutement";')
    last_id = pg_cursor.fetchone()[0]
    return last_id + 1


def load_ville_mapping(pg_cursor):
    pg_cursor.execute("SELECT LOWER(nom_pays), pays_id FROM public.dim_pays")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def get_pays_id_from_country(pg_cursor, country_name):
    if not country_name:
        return None
    pg_cursor.execute("SELECT pays_id FROM public.dim_pays WHERE LOWER(nom_pays) = LOWER(%s)", (country_name,))
    result = pg_cursor.fetchone()
    return result[0] if result else None

def load_offreemploi_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_offre_emploi, offre_emploi_id FROM public.dim_offre_emploi")
    return {row[0].strip().lower(): row[1] for row in pg_cursor.fetchall()}

def load_offre_etude_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_offre_etude, offre_etude_id FROM public.dim_offre_etude")
    return {row[0].strip().lower(): row[1] for row in pg_cursor.fetchall()}

def load_formation_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_formation, formation_id FROM public.dim_formation")
    return {row[0].strip().lower(): row[1] for row in pg_cursor.fetchall()}

def get_mongodb_collections():
    mongo_client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = mongo_client["PowerBi"]
    return (
        db["dossiers"],
        db["frontusers"],
        db["users"],
        db["offredemplois"],
        db["offredetudes"],
        db["formations"],
        db["factures"]
    )

def format_date_only(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value).split("T")[0] if value else None

def get_client_pk_from_profile(profile_id, frontusers_collection, pg_cursor):
    frontuser = frontusers_collection.find_one({"_id": profile_id})
    if frontuser and "matricule" in frontuser:
        return get_client_pk_by_matricule(pg_cursor, frontuser["matricule"])
    return None

def get_offre_pk_from_id(oid, mongo_collection, titre_to_pk):
    if not oid:
        return None, None, None
    doc = mongo_collection.find_one({"_id": ObjectId(oid)})
    if not doc:
        return None, None, None
    titre = (doc.get("titre") or "").strip().lower()
    min_salaire = doc.get("minSalaire")
    max_salaire = doc.get("maxSalaire")
    return titre_to_pk.get(titre, None), min_salaire, max_salaire

def get_services_for_dossier(dossier_id, factures_collection, pg_cursor):
    facture = factures_collection.find_one({"dossierId": dossier_id})
    if not facture or "services" not in facture:
        return []
    
    return [s.get("prix") for s in facture["services"]]


def get_formation_title_and_price_by_id(formation_id, formations_collection):
    formation_object_id = ObjectId(formation_id)
    matched_formation = formations_collection.find_one({"_id": formation_object_id})
    if matched_formation:
        titre_formation = matched_formation.get("titreFormation")
        prix = matched_formation.get("prix")
        return titre_formation, prix
    else:
        return None, None

def fetch_country_from_osm(city_name):
    try:
        time.sleep(1)
        encoded_city = quote(city_name)
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={encoded_city}"
        
        headers = {
            'User-Agent': 'DigitalCook/1.0 (khmiriiheb3@gmail.com)',
            'Accept-Language': 'en'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Error fetching data from Nominatim for {city_name}: {response.status_code}")
            return None
            
        data = response.json()
        if not data:
            logger.warning(f"No data found for city: {city_name}")
            return None

        display_name = data[0].get("display_name", "")
        country = display_name.split(",")[-1].strip()
        
        country_parts = country.split()
        if len(country_parts) > 1 and country_parts[-1].isupper():
            country = country_parts[-1]
        
        logger.info(f"Extracted country for {city_name}: {country}")
        return country
            
    except Exception as e:
        logger.error(f"Error fetching data from Nominatim for {city_name}: {e}")
        return None

def get_name_lastname_from_users(charge_daffaire_id, users_collection):
    if not charge_daffaire_id:
        return None, None
    user = users_collection.find_one({"_id": charge_daffaire_id})
    if not user:
        return None, None
    return user.get("name"), user.get("last_name")

def query_dim_influencer_for_name(pg_cursor, name, last_name):
    if not name or not last_name:
        return None
    pg_cursor.execute(
        "SELECT recruteur_id FROM public.dim_recruteur WHERE nom_recruteur = %s AND prenom_recruteur = %s",
        (name, last_name)
    )
    result = pg_cursor.fetchone()
    return result[0] if result else None

def get_date_pk_from_date(pg_cursor, date_value):
    if not date_value:
        return None
    date_str = format_date_only(date_value)
    pg_cursor.execute("SELECT date_id FROM public.dim_dates WHERE code_date = %s", (date_str,))
    result = pg_cursor.fetchone()
    return result[0] if result else None

def generate_fact_code(index):
    return f"fact{index:04d}"

def get_formation_pk_by_title(titre_formation, pg_cursor):
    pg_cursor.execute(
        "SELECT formation_id FROM public.dim_formation WHERE titre_formation = %s", (titre_formation,)
    )
    result = pg_cursor.fetchone()
    return result[0] if result else None

def upsert_fact_dossier(pg_cursor, dossier_pk, client_pk, fact_code, current_step, type_de_contrat,
                         influencer_pk, destination_pk, date_depart_pk,
                         offre_emploi_step2, offre_etude_step2,
                         offre_emploi_step4, offre_etude_step4, service_prix,
                         formation_pk, formation_prix, created_at, updated_at, minSalaire, maxSalaire,
                         status):
    pg_cursor.execute("""
        INSERT INTO public.fact_recrutement(
            dossier_id, client_id, code_fact, etape_actuelle, type_contrat, recruteur_id, destination_id, date_depart_id,
            offre_emploi_step2_id, offre_etude_step2_id, offre_emploi_step4_id, offre_etude_step4_id
                      , service_prix, formation_id, prix_formation, date_creation, date_mise_a_jour,
            minsalaire, maxsalaire,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dossier_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            code_fact = EXCLUDED.code_fact,
            etape_actuelle = EXCLUDED.etape_actuelle,
            type_contrat = EXCLUDED.type_contrat,
            recruteur_id = EXCLUDED.recruteur_id,
            destination_id = EXCLUDED.destination_id,
            date_depart_id = EXCLUDED.date_depart_id,
            offre_emploi_step2_id = EXCLUDED.offre_emploi_step2_id,
            offre_etude_step2_id = EXCLUDED.offre_etude_step2_id,
            offre_emploi_step4_id = EXCLUDED.offre_emploi_step4_id,
            offre_etude_step4_id = EXCLUDED.offre_etude_step4_id,
            service_prix = EXCLUDED.service_prix,
            formation_id = EXCLUDED.formation_id,
            prix_formation = EXCLUDED.prix_formation,
            date_creation = EXCLUDED.date_creation,
            date_mise_a_jour = EXCLUDED.date_mise_a_jour,
            minsalaire = EXCLUDED.minsalaire,
            maxsalaire = EXCLUDED.maxsalaire,
            status = EXCLUDED.status;
    """, (
        dossier_pk, client_pk, fact_code, current_step, type_de_contrat, influencer_pk,
        destination_pk, date_depart_pk,
        offre_emploi_step2[0] if isinstance(offre_emploi_step2, tuple) else offre_emploi_step2,
        offre_etude_step2[0] if isinstance(offre_etude_step2, tuple) else offre_etude_step2,
        offre_emploi_step4[0] if isinstance(offre_emploi_step4, tuple) else offre_emploi_step4,
        offre_etude_step4[0] if isinstance(offre_etude_step4, tuple) else offre_etude_step4,
          service_prix,
        formation_pk, formation_prix, created_at, updated_at,
        minSalaire, maxSalaire,
        status
    ))


def extract_fields():
    pg_cursor, pg_conn = get_postgres_cursor()
    dossier_pk = get_next_dossier_pk(pg_cursor)
    (
        dossiers_collection,
        frontusers_collection,
        users_collection,
        offredemplois_collection,
        offredetudes_collection,
        formations_collection,
        factures_collection
    ) = get_mongodb_collections()

    ville_name_to_pk = load_ville_mapping(pg_cursor)
    offre_titre_to_pk = load_offreemploi_mapping(pg_cursor)
    offre_etude_titre_to_pk = load_offre_etude_mapping(pg_cursor)

    dossiers = dossiers_collection.find()

    collections_factcode_counters = {
        "dossiers": 1
    }
    fact_code_seen = set()
    fact_code_date_seen = set()
    created_at_seen = set()
    updated_at_seen = set()
    fact_code_seen1 = set()
    fact_code_status_seen = set()

    def generate_collection_fact_code():
        counter = collections_factcode_counters["dossiers"]
        fact_code = f"fact{str(counter).zfill(4)}"
        collections_factcode_counters["dossiers"] += 1
        return fact_code

    for doc in dossiers:
        profile_id = doc.get('profile')
        created_at = doc.get("created_at")
        updated_at = doc.get("updated_at")
        created_at_str = get_date_pk_from_date(pg_cursor, created_at) if created_at not in created_at_seen else None
        updated_at_str = get_date_pk_from_date(pg_cursor, updated_at) if updated_at not in updated_at_seen else None
        client_pk = get_client_pk_from_profile(profile_id, frontusers_collection, pg_cursor)
        if not client_pk:
            continue

        fact_code = generate_collection_fact_code()
        current_step = doc.get("currentStep")
        type_de_contrat = doc.get("firstStep", {}).get("typeDeContrat")
        charge_daffaire_id = doc.get("chargeDaffaire")

        name, last_name = get_name_lastname_from_users(charge_daffaire_id, users_collection)
        recruteur_id = query_dim_influencer_for_name(pg_cursor, name, last_name)

        raw_date_depart = doc.get("firstStep", {}).get("dateDepart")
        date_depart_str = format_date_only(raw_date_depart)
        date_depart_pk = get_date_pk_from_date(pg_cursor, date_depart_str)
        destination_list = doc.get("firstStep", {}).get("destination", [])

        valid_destinations = []
        for destination in destination_list:
            if not destination:
                continue
            
            destination_lower = destination.lower()
            if destination_lower in ville_name_to_pk:
                valid_destinations.append(destination)
            else:
                logger.warning(f"Destination non trouvée dans le mapping: {destination}")
                country = fetch_country_from_osm(destination)
                if country:
                    pays_id = get_pays_id_from_country(pg_cursor, country)
                    if pays_id:
                        ville_name_to_pk[destination_lower] = pays_id
                        valid_destinations.append(destination)
                        logger.info(f"Destination ajoutée au mapping: {destination} -> {country}")
                    else:
                        logger.error(f"Pays non trouvé dans dim_pays: {country}")
                else:
                    logger.error(f"Impossible de récupérer le pays pour: {destination}")

        offres_emploi_step2_ids = doc.get("secondStep", {}).get("selectedOffersDemploi", [])
        offres_etude_step2_ids = doc.get("secondStep", {}).get("selectedOffersDetude", [])
        selected_offre_emploi_step4_id = doc.get("fourthStep", {}).get("selectedOfferDemploi")
        selected_offre_etude_step4_id = doc.get("fourthStep", {}).get("selectedOfferDetude")
        formations = doc.get("thirdStep", {}).get("formations", [])

        formation_pks = []
        formation_prix = []
        for formation in formations:
            formation_id = formation.get("formation")
            if formation_id:
                titre_formation, prix = get_formation_title_and_price_by_id(formation_id, formations_collection)
                if titre_formation and prix is not None:
                    formation_pk = get_formation_pk_by_title(titre_formation, pg_cursor)
                    formation_pks.append(formation_pk)
                    formation_prix.append(prix)

        selected_offre_emploi_step4, min_salaire_step4, max_salaire_step4 = (
            get_offre_pk_from_id(selected_offre_emploi_step4_id, offredemplois_collection, offre_titre_to_pk)
            if selected_offre_emploi_step4_id else (None, None, None)
        )

        offres_emploi_step2 = [
            get_offre_pk_from_id(oid, offredemplois_collection, offre_titre_to_pk) for oid in offres_emploi_step2_ids
        ] if offres_emploi_step2_ids else []

        offres_etude_step2 = [
            get_offre_pk_from_id(oid, offredetudes_collection, offre_etude_titre_to_pk) for oid in offres_etude_step2_ids
        ] if offres_etude_step2_ids else []

        selected_offre_etude_step4 = get_offre_pk_from_id(selected_offre_etude_step4_id, offredetudes_collection, offre_etude_titre_to_pk) if selected_offre_etude_step4_id else None

        offres_emploi_step4 = [selected_offre_emploi_step4] if selected_offre_emploi_step4 else []
        offres_etude_step4 = [selected_offre_etude_step4] if selected_offre_etude_step4 else []

        services_info = get_services_for_dossier(doc["_id"], factures_collection, pg_cursor)

        # --- Extraction des statuts ---
        if current_step == 1:
            status = doc.get("firstStep", {}).get("status")
        elif current_step == 2:
            status = doc.get("secondStep", {}).get("status")
        elif current_step == 3:
            status = doc.get("thirdStep", {}).get("status")
        elif current_step == 4:
            status = doc.get("fourthStep", {}).get("status")
        elif current_step == 5:
            status = doc.get("fifthStep", {}).get("status")
        elif current_step == 6:
            status = "accepte" if doc.get("fifthStep", {}).get("status") == "accepte" else None
        else:
            status = None

        max_length = max(
            len(valid_destinations),
            len(offres_emploi_step2),
            len(offres_etude_step2),
            len(offres_emploi_step4),
            len(offres_etude_step4),
            len(services_info),
            len(formation_pks),
            1
        )

        for i in range(max_length):
            current_step_print = current_step if fact_code not in fact_code_seen else None
            fact_code_seen.add(fact_code)

            date_depart_print = date_depart_pk if fact_code not in fact_code_date_seen else None
            fact_code_date_seen.add(fact_code)

            created_at_str = get_date_pk_from_date(pg_cursor, created_at) if created_at not in created_at_seen else None
            created_at_seen.add(created_at)

            updated_at_str = get_date_pk_from_date(pg_cursor, updated_at) if updated_at not in updated_at_seen else None
            updated_at_seen.add(updated_at)

            type_de_contrat_print = type_de_contrat if fact_code not in fact_code_seen1 else None
            fact_code_seen1.add(fact_code)

            status_print = status if fact_code not in fact_code_status_seen else None
            fact_code_status_seen.add(fact_code)

            min_salaire_step4_current = min_salaire_step4 if i < len(offres_emploi_step4) else None
            max_salaire_step4_current = max_salaire_step4 if i < len(offres_emploi_step4) else None

            formation_pk = formation_pks[i] if i < len(formation_pks) else None
            formation_prix_value = formation_prix[i] if i < len(formation_prix) else None

            destination_pk = ville_name_to_pk.get(valid_destinations[i].lower()) if i < len(valid_destinations) else None
            offre_emploi_step2 = offres_emploi_step2[i] if i < len(offres_emploi_step2) else None
            offre_etude_step2 = offres_etude_step2[i] if i < len(offres_etude_step2) else None
            offre_emploi_step4 = offres_emploi_step4[i] if i < len(offres_emploi_step4) else None
            offre_etude_step4 = offres_etude_step4[i] if i < len(offres_etude_step4) else None

            service_prix = services_info[i] if i < len(services_info) else None

            upsert_fact_dossier(
                pg_cursor,
                dossier_pk,
                client_pk,
                fact_code,
                current_step_print,
                type_de_contrat_print,
                recruteur_id,
                destination_pk,
                date_depart_print,
                offre_emploi_step2,
                offre_etude_step2,
                offre_emploi_step4,
                offre_etude_step4,
                service_prix,
                formation_pk,
                formation_prix_value,
                created_at_str,
                updated_at_str,
                min_salaire_step4_current,
                max_salaire_step4_current,
                status_print
            )

            dossier_pk += 1

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close() 

dag = DAG(
    'dag_fact_recrutement',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
)

def run_etl(**kwargs):
    extract_fields()

etl_task = PythonOperator(
    task_id='fact_recrutement',
    python_callable=run_etl,
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


start_task >>etl_task >> end_task