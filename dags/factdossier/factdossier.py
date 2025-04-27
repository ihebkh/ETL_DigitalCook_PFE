from pymongo import MongoClient
from datetime import datetime, timedelta
import psycopg2
from bson import ObjectId
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_cursor():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="iheb",
        password="201JmT1896@",
        host="monserveur-postgres.postgres.database.azure.com",
        port="5432"
    ), None

def get_client_pk_by_matricule(cursor, matricule):
    cursor.execute("SELECT client_id FROM public.dim_client WHERE matricule_client = %s", (str(matricule),))
    result = cursor.fetchone()
    return result[0] if result else None

def get_service_pk_from_nom_service(pg_cursor, nom_service):
    pg_cursor.execute("SELECT service_id FROM public.dim_service WHERE nom_service = %s", (nom_service,))
    result = pg_cursor.fetchone()
    return result[0] if result else None

def load_ville_mapping(pg_cursor):
    pg_cursor.execute("SELECT nom_ville, ville_id FROM public.dim_ville")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_offreemploi_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_offre_emploi, offre_emploi_id FROM public.dim_offreemploi")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_offre_etude_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_offre_etude, offre_etude_id FROM public.dim_offre_etude")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_formation_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre_formation, formation_id FROM public.dim_formation")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_date_mapping(pg_cursor):
    pg_cursor.execute("SELECT date_id, date_id FROM public.dim_dates")
    return {str(row[0]): row[1] for row in pg_cursor.fetchall()}

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
        return None
    doc = mongo_collection.find_one({"_id": oid})
    if not doc:
        return None
    titre = doc.get("titre") or doc.get("titreFormation")
    return titre_to_pk.get(titre, None)

def get_services_for_dossier(dossier_id, factures_collection, pg_cursor):
    facture = factures_collection.find_one({"dossierId": dossier_id})
    if not facture or "services" not in facture:
        return []
    
    result = []
    for s in facture["services"]:
        nom_service = s.get("nomService")
        service_pk = get_service_pk_from_nom_service(pg_cursor, nom_service)
        prix = s.get("prix")
        discount = s.get("discount")
        extra_fee = s.get("extraFee", {}).get("value")
        result.append((service_pk, prix, discount, extra_fee))
    return result

def get_formation_title_and_price_by_id(formation_id, formations_collection):
    formation = formations_collection.find_one({"_id": formation_id})
    if formation:
        titre_formation = formation.get("titre_formation")
        prix = formation.get("prix")
        return titre_formation, prix
    else:
        return None, None


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
                         offre_emploi_step4, offre_etude_step4, service_pk, service_prix, service_discount, service_extra,
                         formation_pk, formation_prix, created_at, updated_at):
    pg_cursor.execute("""
        INSERT INTO public.fact_dossier (
            dossier_id, client_id, code_fact, etape_actuelle, type_contrat, recruteur_id, destination_id , date_depart_id,
            offre_emploi_step2_id, offre_etude_step2_id, offre_emploi_step4_id, offre_etude_step4_id,
            service_id, service_prix, remise_service, extra_service, formation_id, prix_formation, date_creation, date_mise_a_jour
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dossier_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            code_fact = EXCLUDED.code_fact,
            etape_actuelle = EXCLUDED.etape_actuelle,
            type_contrat = EXCLUDED.type_contrat,
            recruteur_id = EXCLUDED.recruteur_id,
            destination_id  = EXCLUDED.destination_id ,
            date_depart_id = EXCLUDED.date_depart_id,
            offre_emploi_step2_id = EXCLUDED.offre_emploi_step2_id,
            offre_etude_step2_id = EXCLUDED.offre_etude_step2_id,
            offre_emploi_step4_id = EXCLUDED.offre_emploi_step4_id,
            offre_etude_step4_id = EXCLUDED.offre_etude_step4_id,
            service_id = EXCLUDED.service_id,
            service_prix = EXCLUDED.service_prix,
            remise_service = EXCLUDED.remise_service,
            extra_service = EXCLUDED.extra_service,
            formation_id = EXCLUDED.formation_id,
            prix_formation = EXCLUDED.prix_formation,
            date_creation = EXCLUDED.date_creation,
            date_mise_a_jour = EXCLUDED.date_mise_a_jour;
    """, (
        dossier_pk, client_pk, fact_code, current_step, type_de_contrat, influencer_pk,
        destination_pk, date_depart_pk,
        offre_emploi_step2, offre_etude_step2,
        offre_emploi_step4, offre_etude_step4,
        service_pk, service_prix, service_discount, service_extra,
        formation_pk, formation_prix, created_at, updated_at
    ))


def extract_fields():
    
    pg_conn, _ = get_postgres_cursor()
    pg_cursor = pg_conn.cursor()
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
    datecode_to_pk = load_date_mapping(pg_cursor)

    dossiers = dossiers_collection.find()
    client_factcode_map = {}
    current_fact_index = 1
    dossier_pk = 1
    fact_code_seen = set()         
    fact_code_date_seen = set()   
    created_at_seen = set()
    updated_at_seen = set()
    fact_code_seen1 = set()

    for doc in dossiers:
        profile_id = doc.get('profile')
        created_at = doc.get("created_at")
        updated_at = doc.get("updated_at")
        created_at_str = get_date_pk_from_date(pg_cursor, created_at) if created_at not in created_at_seen else None
        updated_at_str = get_date_pk_from_date(pg_cursor, updated_at) if updated_at not in updated_at_seen else None
        client_pk = get_client_pk_from_profile(profile_id, frontusers_collection, pg_cursor)
        if not client_pk:
            continue

        if client_pk not in client_factcode_map:
            client_factcode_map[client_pk] = generate_fact_code(current_fact_index)
            current_fact_index += 1

        fact_code = client_factcode_map[client_pk]
        current_step = doc.get("currentStep")
        type_de_contrat = doc.get("firstStep", {}).get("typeDeContrat")
        charge_daffaire_id = doc.get("chargeDaffaire")
        print(client_pk)

        name, last_name = get_name_lastname_from_users(charge_daffaire_id, users_collection)
        influencer_pk = query_dim_influencer_for_name(pg_cursor, name, last_name)
      
        raw_date_depart = doc.get("firstStep", {}).get("dateDepart")
        date_depart_str = format_date_only(raw_date_depart)
        date_depart_pk = datecode_to_pk.get(date_depart_str, None)
        destination_list = doc.get("firstStep", {}).get("destination", [])

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

        offres_emploi_step2 = [get_offre_pk_from_id(oid, offredemplois_collection, offre_titre_to_pk) for oid in offres_emploi_step2_ids] if offres_emploi_step2_ids else []
        offres_etude_step2 = [get_offre_pk_from_id(oid, offredetudes_collection, offre_etude_titre_to_pk) for oid in offres_etude_step2_ids] if offres_etude_step2_ids else []
        selected_offre_emploi_step4 = get_offre_pk_from_id(selected_offre_emploi_step4_id, offredemplois_collection, offre_titre_to_pk) if selected_offre_emploi_step4_id else None
        selected_offre_etude_step4 = get_offre_pk_from_id(selected_offre_etude_step4_id, offredetudes_collection, offre_etude_titre_to_pk) if selected_offre_etude_step4_id else None

        offres_emploi_step4 = [selected_offre_emploi_step4] if selected_offre_emploi_step4 else []
        offres_etude_step4 = [selected_offre_etude_step4] if selected_offre_etude_step4 else []

        services_info = get_services_for_dossier(doc["_id"], factures_collection, pg_cursor)

        max_length = max(
            len(destination_list),
            len(offres_emploi_step2),
            len(offres_etude_step2),
            len(offres_emploi_step4),
            len(offres_etude_step4),
            len(services_info),
            len(formation_pks),
            1
        )

        for i in range(max_length):
            if fact_code not in fact_code_seen:
                current_step_print = current_step
                fact_code_seen.add(fact_code)
            else:
                current_step_print = None

            if fact_code not in fact_code_date_seen:
                date_depart_print = date_depart_pk
                fact_code_date_seen.add(fact_code)
            else:
                date_depart_print = None

            if created_at not in created_at_seen:
                created_at_str = get_date_pk_from_date(pg_cursor, created_at)
                created_at_seen.add(created_at) 
            else:
                created_at_str = None

            if updated_at not in updated_at_seen:
                updated_at_str = get_date_pk_from_date(pg_cursor, updated_at)
                updated_at_seen.add(updated_at)
            else:
                updated_at_str = None 

            if fact_code not in fact_code_seen1:
                type_de_contrat_print = type_de_contrat if type_de_contrat else "Type de contrat non défini"
                fact_code_seen1.add(fact_code)
            else:
                type_de_contrat_print = None

            

            formation_pk = formation_pks[i] if i < len(formation_pks) else None
            formation_prix_value = formation_prix[i] if i < len(formation_prix) else None

            upsert_fact_dossier(
                pg_cursor,
                dossier_pk,
                client_pk,
                fact_code,
                current_step_print,
                type_de_contrat_print,
                influencer_pk,
                ville_name_to_pk.get(destination_list[i], None) if i < len(destination_list) else None,
                date_depart_print,
                offres_emploi_step2[i] if i < len(offres_emploi_step2) else None,
                offres_etude_step2[i] if i < len(offres_etude_step2) else None,
                offres_emploi_step4[i] if i < len(offres_emploi_step4) else None,
                offres_etude_step4[i] if i < len(offres_etude_step4) else None,
                service_pk=services_info[i][0] if i < len(services_info) else None,  
                service_prix=services_info[i][1] if i < len(services_info) else None,
                service_discount=services_info[i][2] if i < len(services_info) else None,
                service_extra=services_info[i][3] if i < len(services_info) else None,      
                created_at=created_at_str,
                updated_at=updated_at_str,
                formation_pk=formation_pk,
                formation_prix=formation_prix_value
            )

            dossier_pk += 1

    pg_conn.commit()
    pg_conn.close()

if __name__ == "__main__":
    extract_fields()