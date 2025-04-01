from pymongo import MongoClient
import psycopg2

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["dossiers"], db["users"]

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def load_charge_daffaire_last_names(users_collection):
    users = users_collection.find({}, {"_id": 1, "last_name": 1})
    return {
        str(user["_id"]): user.get("last_name", "").strip()
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

def display_charge_daffaire_with_influencer_pk():
    dossiers_collection, users_collection = get_mongodb_connection()
    charge_map = load_charge_daffaire_last_names(users_collection)
    influencer_map = load_influencer_lastname_to_pk()

    cursor = dossiers_collection.find({}, {"chargeDaffaire": 1})

    print("\n📦 Chargé d'affaire → influencer_pk :\n")

    for i, dossier in enumerate(cursor, start=1):
        charge_id = str(dossier.get("chargeDaffaire", "—"))
        last_name = charge_map.get(charge_id, "").strip().lower()
        influencer_pk = influencer_map.get(last_name, "❌")
        print(f"{i:02d}. {last_name or 'Inconnu'} → influencer_pk : {influencer_pk}")

if __name__ == "__main__":
    display_charge_daffaire_with_influencer_pk()
