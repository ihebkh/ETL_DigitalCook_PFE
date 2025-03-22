import psycopg2
from pymongo import MongoClient
import datetime

def get_postgresql_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "formations"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def extract_formation_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()
    formations = collection.find({}, {"_id": 0, "titreFormation": 1, "dateDebut": 1, "dateFin": 1, 
                                       "domaine": 1, "ville": 1, "adresse": 1, "centreDeFormation": 1, 
                                       "presence": 1, "duree": 1})
    formation_list = []
    for formation in formations:
        formation_list.append(formation)

    client.close()
    return formation_list

def generate_formation_code(index):
    return f"formation{str(index).zfill(4)}"

def convert_to_datetime(date_value):
    if isinstance(date_value, datetime.datetime):
        return date_value
    elif isinstance(date_value, str):
        try:
            return datetime.datetime.strptime(date_value, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    return None

def insert_or_update_formation(formation, formation_code):
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    titreformation = formation.get("titreFormation", "")
    date_debut = convert_to_datetime(formation.get("dateDebut", None))
    date_fin = convert_to_datetime(formation.get("dateFin", None))
    domaine = formation.get("domaine", "")
    ville = formation.get("ville", "")
    adresse = formation.get("adresse", "")
    centreformations = formation.get("centreDeFormation", "")
    presence = formation.get("presence", "")
    duree = formation.get("duree", "")
    query = """
    INSERT INTO dim_formation (formationcode, titreformation, date_debut, date_fin, domaine, ville, adresse, centreformations, presence, duree)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (formationcode) DO UPDATE
    SET titreformation = EXCLUDED.titreformation,
        date_debut = EXCLUDED.date_debut,
        date_fin = EXCLUDED.date_fin,
        domaine = EXCLUDED.domaine,
        ville = EXCLUDED.ville,
        adresse = EXCLUDED.adresse,
        centreformations = EXCLUDED.centreformations,
        presence = EXCLUDED.presence,
        duree = EXCLUDED.duree;
    """
    
    cursor.execute(query, (formation_code, titreformation, date_debut, date_fin, domaine, ville, adresse, centreformations, presence, duree))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    formations = extract_formation_from_mongodb()
    print("Formations extraites de MongoDB avec leurs codes générés:")
    
    for index, formation in enumerate(formations, start=1):
        formation_code = generate_formation_code(index)  #
        print(f"Insertion/Modification de la formation : {formation.get('titreFormation')} - {formation_code}")
        insert_or_update_formation(formation, formation_code)

if __name__ == "__main__":
    main()
