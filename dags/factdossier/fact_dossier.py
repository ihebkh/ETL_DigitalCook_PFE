from pymongo import MongoClient
import psycopg2
from bson import ObjectId

def get_mongodb_collections():
    mongo_client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    mongo_db = mongo_client["PowerBi"]
    return mongo_db["dossiers"], mongo_db["frontusers"], mongo_db["formations"], mongo_db["users"], mongo_db["offredemplois"]

def get_postgres_cursor():
    pg_conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return pg_conn, pg_conn.cursor()

def get_client_pk_by_matricule(cursor, matricule):
    cursor.execute("SELECT client_pk FROM public.dim_client WHERE matricule = %s", (str(matricule),))
    return cursor.fetchone()

def get_ville_pk_by_name(cursor, destination_name):
    cursor.execute("SELECT ville_pk FROM public.dim_ville WHERE LOWER(name) = LOWER(%s)", (destination_name,))
    return cursor.fetchone()

def get_date_pk(cursor, date_obj):
    if not date_obj:
        return None
    cursor.execute("SELECT date_pk FROM public.dim_dates WHERE datecode = %s", (date_obj,))
    return cursor.fetchone()

def get_formation_pk_by_title(cursor, title):
    cursor.execute("SELECT formation_pk FROM public.dim_formation WHERE LOWER(titreformation) = LOWER(%s)", (title,))
    return cursor.fetchone()

def get_offre_pk_by_titre(cursor, titre):
    cursor.execute("SELECT offre_pk FROM public.offreemploi WHERE LOWER(titre) = LOWER(%s)", (titre,))
    return cursor.fetchone()

def get_influencer_pk(cursor, nom, prenom):
    cursor.execute("SELECT influencer_pk FROM public.dim_influencer WHERE LOWER(nom) = LOWER(%s) AND LOWER(prenom) = LOWER(%s)", (nom, prenom))
    return cursor.fetchone()

def generate_fact_code(counter):
    return f"FACT{counter:04d}"

def generate_sequence_with_all_fields():
    dossiers, frontusers, formations, users, offres = get_mongodb_collections()
    pg_conn, pg_cursor = get_postgres_cursor()

    index = 1
    fact_counter = 1

    for dossier in dossiers.find({}):
        profile_id = dossier.get("profile")
        current_step = dossier.get("currentStep", None)
        destination_list = dossier.get("firstStep", {}).get("destination", [])
        type_de_demande = dossier.get("firstStep", {}).get("typeDeDemande", None)
        date_depart = dossier.get("firstStep", {}).get("dateDepart", None)
        formations_list = dossier.get("thirdStep", {}).get("formations", [])

        selected_offers_detude = dossier.get("secondStep", {}).get("selectedOffersDetude", [])
        if isinstance(selected_offers_detude, ObjectId):
            selected_offers_detude = [selected_offers_detude]
        selected_offer_ids = [str(oid) for oid in selected_offers_detude if isinstance(oid, ObjectId)]

        selected_offers_demploi = dossier.get("secondStep", {}).get("selectedOffersDemploi", [])
        if isinstance(selected_offers_demploi, ObjectId):
            selected_offers_demploi = [selected_offers_demploi]
        selected_employ_ids = [str(oid) for oid in selected_offers_demploi if isinstance(oid, ObjectId)]

        fourth_offer_detude = dossier.get("fourthStep", {}).get("selectedOfferDetude")
        fourth_offer_detude_id = str(fourth_offer_detude) if isinstance(fourth_offer_detude, ObjectId) else None

        fourth_offer_demploi = dossier.get("fourthStep", {}).get("selectedOfferDemploi")
        fourth_offer_demploi_id = str(fourth_offer_demploi) if isinstance(fourth_offer_demploi, ObjectId) else None

        charge_daffaire_id = dossier.get("chargeDaffaire")
        influencer_pk = None
        if isinstance(charge_daffaire_id, ObjectId):
            user_doc = users.find_one({"_id": charge_daffaire_id}, {"name": 1, "last_name": 1})
            if user_doc:
                nom = user_doc.get("name", "")
                prenom = user_doc.get("last_name", "")
                result = get_influencer_pk(pg_cursor, nom, prenom)
                if result:
                    influencer_pk = result[0]

        date_pk = None
        if date_depart:
            result = get_date_pk(pg_cursor, date_depart)
            if result:
                date_pk = result[0]

        if isinstance(profile_id, ObjectId):
            frontuser = frontusers.find_one({"_id": profile_id}, {"matricule": 1})
            if frontuser and "matricule" in frontuser:
                result = get_client_pk_by_matricule(pg_cursor, frontuser["matricule"])
                if result:
                    client_pk = result[0]
                    fact_code = generate_fact_code(fact_counter)
                    fact_counter += 1

                    max_length = max(len(destination_list), len(formations_list), len(selected_offer_ids), len(selected_employ_ids), 1)

                    for i in range(max_length):
                        destination = destination_list[i] if i < len(destination_list) else None
                        selected_offer_detude_id = selected_offer_ids[i] if i < len(selected_offer_ids) else None
                        selected_offer_demploi_id = selected_employ_ids[i] if i < len(selected_employ_ids) else None

                        step_display = current_step if i == 0 else None
                        type_demande_display = type_de_demande if i == 0 else None
                        date_pk_display = date_pk if i == 0 else None
                        fourth_detude_display = fourth_offer_detude_id if i == 0 else None
                        fourth_demploi_display = fourth_offer_demploi_id if i == 0 else None
                        influencer_display = influencer_pk if i == 0 else None

                        ville_pk = None
                        if destination:
                            ville_result = get_ville_pk_by_name(pg_cursor, destination)
                            if ville_result:
                                ville_pk = ville_result[0]

                        formation_pk = None
                        hours_per_week = None
                        extra_fee_label = None
                        discount = None
                        prix = None

                        if i < len(formations_list):
                            formation_data = formations_list[i]
                            formation_id = formation_data.get("formation")
                            hours_per_week = formation_data.get("hoursPerWeek")
                            discount = formation_data.get("discount")
                            extra_fee_label = formation_data.get("extraFee", {}).get("label")

                            if formation_id:
                                formation_doc = formations.find_one({"_id": ObjectId(formation_id)})
                                if formation_doc:
                                    prix = formation_doc.get("prix")
                                    titre = formation_doc.get("titreFormation")
                                    pg_result = get_formation_pk_by_title(pg_cursor, titre)
                                    if pg_result:
                                        formation_pk = pg_result[0]

                        offre_pk_selected = None
                        if selected_offer_demploi_id:
                            offre_doc = offres.find_one({"_id": ObjectId(selected_offer_demploi_id)})
                            if offre_doc:
                                titre = offre_doc.get("titre")
                                pg_result = get_offre_pk_by_titre(pg_cursor, titre)
                                if pg_result:
                                    offre_pk_selected = pg_result[0]

                        offre_pk_fourth = None
                        if fourth_offer_demploi_id:
                            offre_doc = offres.find_one({"_id": ObjectId(fourth_offer_demploi_id)})
                            if offre_doc:
                                titre = offre_doc.get("titre")
                                pg_result = get_offre_pk_by_titre(pg_cursor, titre)
                                if pg_result:
                                    offre_pk_fourth = pg_result[0]

                        print(
                            f"Séquence {index} → {fact_code} | client_pk : {client_pk} | currentStep : {step_display} "
                            f"| typeDeDemande : {type_demande_display} | date_pk : {date_pk_display} "
                            f"| destination_pk : {ville_pk} | formation_pk : {formation_pk} "
                            f"| hoursPerWeek : {hours_per_week} | extraFeeLabel : {extra_fee_label} "
                            f"| discount : {discount} | prixFormation : {prix} "
                            f"| selectedOfferDetude_id : {selected_offer_detude_id} | fourthStep_selectedOfferDetude_id : {fourth_detude_display} "
                            f"| selectedOfferDemploi_pk : {offre_pk_selected} | fourthStep_selectedOfferDemploi_pk : {offre_pk_fourth} "
                            f"| influencer_fk : {influencer_display}"
                        )
                        index += 1

    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    generate_sequence_with_all_fields()
