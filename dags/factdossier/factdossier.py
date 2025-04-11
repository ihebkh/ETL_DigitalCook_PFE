from pymongo import MongoClient
from datetime import datetime
import psycopg2

def get_postgres_cursor():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="iheb",
        password="201JmT1896@",
        host="monserveur-postgres.postgres.database.azure.com",
        port="5432"
    ), None

def get_client_pk_by_matricule(cursor, matricule):
    cursor.execute("SELECT client_pk FROM public.dim_client WHERE matricule = %s", (str(matricule),))
    result = cursor.fetchone()
    return result[0] if result else None

def load_ville_mapping(pg_cursor):
    pg_cursor.execute("SELECT name, ville_pk FROM public.dim_ville")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_offreemploi_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre, offre_pk FROM public.dim_offreemploi")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_offre_etude_mapping(pg_cursor):
    pg_cursor.execute("SELECT titre, offre_pk FROM public.dim_offre_etude")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_formation_mapping(pg_cursor):
    pg_cursor.execute("SELECT titreformation, formation_pk FROM public.dim_formation")
    return {row[0]: row[1] for row in pg_cursor.fetchall()}

def load_date_mapping(pg_cursor):
    pg_cursor.execute("SELECT datecode, date_pk FROM public.dim_dates")
    return {str(row[0]): row[1] for row in pg_cursor.fetchall()}

def get_mongodb_collections():
    mongo_client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = mongo_client["PowerBi"]
    return db["dossiers"], db["frontusers"], db["offredemplois"], db["offredetudes"], db["formations"], db["factures"]

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

def get_formation_infos_from_id(oid, formations_collection, titre_to_pk):
    if not oid:
        return None, None, None
    doc = formations_collection.find_one({"_id": oid})
    if not doc:
        return None, None, None
    titre = doc.get("titreFormation")
    formation_pk = titre_to_pk.get(titre, None)
    prix = doc.get("prix")
    hours = doc.get("hoursNumber")
    return formation_pk, prix, hours

def get_services_for_dossier(dossier_id, factures_collection):
    facture = factures_collection.find_one({"dossierId": dossier_id})
    if not facture or "services" not in facture:
        return []

    result = []
    for s in facture["services"]:
        nom = s.get("nomService")
        prix = s.get("prix")
        discount = s.get("discount")
        extra_fee = s.get("extraFee", {}).get("label")
        result.append((nom, prix, discount, extra_fee))
    return result

def generate_fact_code(index):
    return f"fact{index:04d}"



def get_offre_emploi_salaries(oid, offredemplois_collection):
    if not oid:
        return None, None
    doc = offredemplois_collection.find_one({"_id": oid})
    if not doc:
        return None, None
    min_salaire = doc.get("minSalaire")
    max_salaire = doc.get("maxSalaire")
    return min_salaire, max_salaire

def query_dim_influencer_for_name(dim_influencer_cursor, name):
    query = "SELECT influencer_pk FROM public.dim_influencer WHERE nom = %s OR prenom = %s"
    dim_influencer_cursor.execute(query, (name, name))
    return dim_influencer_cursor.fetchone()




def extract_fields():
    dossiers_collection, frontusers_collection, offredemplois_collection, offredetudes_collection, formations_collection, factures_collection = get_mongodb_collections()
    pg_conn, _ = get_postgres_cursor()
    pg_cursor = pg_conn.cursor()

    ville_name_to_pk = load_ville_mapping(pg_cursor)
    offre_titre_to_pk = load_offreemploi_mapping(pg_cursor)
    offre_etude_titre_to_pk = load_offre_etude_mapping(pg_cursor)
    formation_titre_to_pk = load_formation_mapping(pg_cursor)
    datecode_to_pk = load_date_mapping(pg_cursor)

    print("client_pk;fact_code;sequence;current_step;type_de_contrat;charge_daffaire_id;destination_pk;date_depart_pk;formation_pk;prix_formation;hoursNumber;offre_emploi_step2;offre_etude_step2;offre_emploi_step4;offre_etude_step4;service_nom;service_prix;service_discount;service_extra;influencer_pk")

    dossiers = dossiers_collection.find()
    client_factcode_map = {}
    current_fact_index = 1
    global_sequence = 1

    for doc in dossiers:
        profile_id = doc.get('profile')
        client_pk = get_client_pk_from_profile(profile_id, frontusers_collection, pg_cursor)
        if not client_pk:
            continue

        if client_pk not in client_factcode_map:
            client_factcode_map[client_pk] = generate_fact_code(current_fact_index)
            current_fact_index += 1

        fact_code = client_factcode_map[client_pk]
        current_step = doc.get("currentStep")
        
        # Remplacer "typeDeDemande" par "typeDeContrat"
        type_de_contrat = doc.get("firstStep", {}).get("typeDeContrat")
        
        # Extraire seulement l'ID de chargeDaffaire (_id)
        charge_daffaire_id = doc.get("chargeDaffaire")  # Afficher uniquement l'ID (_id)
       
        raw_date_depart = doc.get("firstStep", {}).get("dateDepart")
        date_depart_str = format_date_only(raw_date_depart)
        date_depart_pk = datecode_to_pk.get(date_depart_str, None)
        destination_list = doc.get("firstStep", {}).get("destination", [])

        formations = [
            get_formation_infos_from_id(f.get("_id"), formations_collection, formation_titre_to_pk)
            for f in doc.get("thirdStep", {}).get("formations", [])
            if f.get("_id")
        ]

        offres_emploi_step2_ids = doc.get("secondStep", {}).get("selectedOffersDemploi", [])
        offres_etude_step2_ids = doc.get("secondStep", {}).get("selectedOffersDetude", [])
        selected_offre_emploi_step4_id = doc.get("fourthStep", {}).get("selectedOfferDemploi")
        selected_offre_etude_step4_id = doc.get("fourthStep", {}).get("selectedOfferDetude")

        offres_emploi_step2 = [get_offre_pk_from_id(oid, offredemplois_collection, offre_titre_to_pk) for oid in offres_emploi_step2_ids]
        offre_emploi_step4 = get_offre_pk_from_id(selected_offre_emploi_step4_id, offredemplois_collection, offre_titre_to_pk)
        offres_etude_step2 = [get_offre_pk_from_id(oid, offredetudes_collection, offre_etude_titre_to_pk) for oid in offres_etude_step2_ids]
        offre_etude_step4 = get_offre_pk_from_id(selected_offre_etude_step4_id, offredetudes_collection, offre_etude_titre_to_pk)

        offres_emploi_step4 = [offre_emploi_step4] if offre_emploi_step4 else []
        offres_etude_step4 = [offre_etude_step4] if offre_etude_step4 else []

        services_info = get_services_for_dossier(doc["_id"], factures_collection)

        max_length = max(
            len(destination_list),
            len(formations),
            len(offres_emploi_step2),
            len(offres_etude_step2),
            len(offres_emploi_step4),
            len(offres_etude_step4),
            len(services_info),
            1
        )

        for i in range(max_length):
            show_main_data = i == 0

            # Créer une seule chaîne de caractères avec toutes les valeurs séparées par des points-virgules
            output = f"{client_pk if show_main_data else None};" \
                     f"{fact_code if show_main_data else None};" \
                     f"{global_sequence};" \
                     f"{current_step if show_main_data else None};" \
                     f"{type_de_contrat if show_main_data else None};" \
                     f"{charge_daffaire_id if show_main_data else None};"\
                     f"{ville_name_to_pk.get(destination_list[i], None) if i < len(destination_list) else None};" \
                     f"{date_depart_pk if show_main_data else None};"

            formation_pk, prix, hours = formations[i] if i < len(formations) else (None, None, None)
            output += f"{formation_pk if show_main_data else None};" \
                      f"{prix if show_main_data else None};" \
                      f"{hours if show_main_data else None};"

            output += f"{offres_emploi_step2[i] if i < len(offres_emploi_step2) else None};" \
                      f"{offres_etude_step2[i] if i < len(offres_etude_step2) else None};" \
                      f"{offres_emploi_step4[i] if i < len(offres_emploi_step4) else None};" \
                      f"{offres_etude_step4[i] if i < len(offres_etude_step4) else None};"

            # Service fields
            if i < len(services_info):
                output += f"{services_info[i][0]};{services_info[i][1]};{services_info[i][2]};{services_info[i][3]};"
            else:
                output += "None;None;None;None;"

            # Imprimer la ligne complète
            print(output)
            global_sequence += 1

    pg_conn.close()

if __name__ == "__main__":
    extract_fields()
