from pymongo import MongoClient
from bson import ObjectId
import psycopg2

# Connexion MongoDB
def get_mongodb_collections():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["offredetudes"], db["universities"]

# Connexion PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

# Mapping MongoDB: university_id → nom
def load_universite_id_to_name(universities_collection):
    cursor = universities_collection.find({}, {"_id": 1, "nom": 1})
    return {str(doc["_id"]): doc.get("nom", "").strip().lower() for doc in cursor}

# Mapping PostgreSQL: nom → codeuniversite
def load_nom_to_codeuniversite():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT nom, codeuniversite FROM public.dim_universite;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {nom.strip().lower(): code for nom, code in rows if nom and code}

# Générateur de codeoffre: ofed0001, ofed0002...
def generate_code_offre(counter: int):
    return f"ofed{counter:04d}"

# Charger les codeoffre existants pour éviter les doublons
def load_existing_codeoffres():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT codeoffre FROM public.dim_offre;")
    codes = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return set(codes)

# Insertion ou mise à jour dans dim_offre
def insert_or_update_offres_batch(offres_data):
    existing_codes = load_existing_codeoffres()
    conn = get_postgres_connection()
    cur = conn.cursor()

    counter = len(existing_codes) + 1
    total = 0

    for titre, codeuniv, dispo, financement in offres_data:
        # Génération d'un codeoffre unique
        while True:
            code = generate_code_offre(counter)
            counter += 1
            if code not in existing_codes:
                existing_codes.add(code)
                break

        cur.execute("""
            INSERT INTO public.dim_offre (codeoffre, titre, codeuniversite, disponibilite, financement)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (codeoffre) DO UPDATE
            SET titre = EXCLUDED.titre,
                codeuniversite = EXCLUDED.codeuniversite,
                disponibilite = EXCLUDED.disponibilite,
                financement = EXCLUDED.financement;
        """, (code, titre, codeuniv, dispo, financement))
        total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {total} offres insérées ou mises à jour via codeoffre.")

# Extraction principale
def extract_offres_etudes():
    offres_collection, universities_collection = get_mongodb_collections()
    id_to_name = load_universite_id_to_name(universities_collection)
    name_to_code = load_nom_to_codeuniversite()

    cursor = offres_collection.find({}, {
        "titre": 1,
        "university": 1,
        "disponibilite": 1,
        "criterias": 1
    })

    offres_to_insert = []

    for doc in cursor:
        titre = doc.get("titre", "—")
        university_id = str(doc.get("university", "—"))
        university_name = id_to_name.get(university_id, "—").lower()
        university_code = name_to_code.get(university_name, "Non trouvé")
        disponibilite = doc.get("disponibilite", "—")

        financement = "—"
        for crit in doc.get("criterias", []):
            if crit.get("label", "").lower() in ["financement", "finance", "funding"]:
                financement = crit.get("value", "—")
                break

        offres_to_insert.append((titre, university_code, disponibilite, financement))

    insert_or_update_offres_batch(offres_to_insert)

# Exécution
if __name__ == "__main__":
    extract_offres_etudes()
