from pymongo import MongoClient
import psycopg2

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["dossiers"], db["users"], db["formations"]

def load_ville_name_to_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, ville_pk FROM public.dim_ville WHERE name IS NOT NULL;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {name.strip(): pk for name, pk in rows if name}

def load_matricule_to_clientpk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT client_pk, matricule FROM public.dim_client WHERE matricule IS NOT NULL;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {str(matricule).strip(): client_pk for client_pk, matricule in rows if matricule}

def load_formationid_to_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT formationcode, titreformation, formation_pk FROM public.dim_formation;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {code.strip(): pk for code, titre, pk in rows if code}

def load_charge_daffaire_last_names(users_collection):
    users = users_collection.find({}, {"_id": 1, "last_name": 1})
    return {
        str(user["_id"]): user.get("last_name", "").strip().lower()
        for user in users
    }

def load_influencer_lastname_to_pk():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT influencer_pk, prenom FROM public.dim_influencer;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {
        prenom.strip().lower(): pk for pk, prenom in rows if prenom
    }

def display_dossiers():
    dossiers_collection, users_collection, formations_collection = get_mongodb_connection()
    villes_dict = load_ville_name_to_pk()
    matricule_to_clientpk = load_matricule_to_clientpk()
    formationid_to_pk = load_formationid_to_pk()
    charge_map = load_charge_daffaire_last_names(users_collection)
    influencer_map = load_influencer_lastname_to_pk()

    cursor = dossiers_collection.find({}, {
        "matricule": 1,
        "currentStep": 1,
        "created_at": 1,
        "firstStep.status": 1,
        "firstStep.destination": 1,
        "firstStep.typeDeContrat": 1,
        "firstStep.typeDeDemande": 1,
        "firstStep.dateDepart": 1,
        "secondStep.status": 1,
        "secondStep.selectedOffersDemploi": 1,
        "secondStep.selectedOffersDetude": 1,
        "thirdStep.formations": 1,
        "chargeDaffaire": 1
    })

    print("\n📦 Dossiers avec client_pk, charge d'affaire et influencer_pk :\n")

    for i, dossier in enumerate(cursor, start=1):
        matricule = str(dossier.get("matricule", "❌")).strip()
        client_pk = matricule_to_clientpk.get(matricule, "❌")
        current_step = dossier.get("currentStep", "❓")
        created_at = dossier.get("created_at", "❓")

        chargedaffaire_id = str(dossier.get("chargeDaffaire", "❓"))
        last_name = charge_map.get(chargedaffaire_id, "").lower()
        influencer_pk = influencer_map.get(last_name, "❌")

        first_step = dossier.get("firstStep", {})
        status = first_step.get("status", "❓")
        destinations = first_step.get("destination", [])
        type_contrat = first_step.get("typeDeContrat", "❓")
        type_demande = first_step.get("typeDeDemande", "❓")
        date_depart = first_step.get("dateDepart", "❓")

        ville_pks = [str(villes_dict.get(dest.strip(), "❌")) for dest in destinations]

        print(f"📁 Dossier {i}:")
        print(f"   🔑 Matricule         : {matricule}")
        print(f"   🧍 client_pk         : {client_pk}")
        print(f"   🧩 currentStep       : {current_step}")
        print(f"   📌 Status            : {status}")
        print(f"   🏙️ Destinations (ville_pk) : {', '.join(ville_pks)}")
        print(f"   🎓 Type de demande   : {type_demande}")
        print(f"   📃 Type de contrat   : {type_contrat}")
        print(f"   🗓️ Date de départ    : {date_depart}")
        print(f"   🕒 Créé le           : {created_at}")
        print(f"   👤 Chargé d'affaire  : {last_name} → influencer_pk = {influencer_pk}")

        second_step = dossier.get("secondStep", {})
        second_status = second_step.get("status", "❓")
        selected_jobs = second_step.get("selectedOffersDemploi", [])
        selected_studies = second_step.get("selectedOffersDetude", [])

        print(f"   🧾 Second Step Status : {second_status}")
        print(f"   💼 Offres d'emploi sélectionnées :")
        for job in selected_jobs:
            print(f"     - {job}")

        print(f"   🎓 Offres d'études sélectionnées :")
        for study in selected_studies:
            print(f"     - {study}")

        formations = dossier.get("thirdStep", {}).get("formations", [])
        if formations:
            print("   📘 Formations:")
            for f in formations:
                formation_id = str(f.get("formation"))
                hours = f.get("hoursPerWeek")
                status = f.get("status")
                extra_fee = f.get("extraFee", {}).get("value", 0)
                formation_pk = formationid_to_pk.get(formation_id, "❌")

                print(f"     • Formation ID     : {formation_id}")
                print(f"       - formation_pk   : {formation_pk}")
                print(f"       - Hours/week     : {hours}")
                print(f"       - Status         : {status}")
                print(f"       - Extra Fee (€)  : {extra_fee}")

        print("-" * 60)

if __name__ == "__main__":
    display_dossiers()
