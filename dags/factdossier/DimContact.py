import psycopg2
from pymongo import MongoClient

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
    MONGO_COLLECTION = "universities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection


def extract_contacts_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()

    universities = collection.find({}, {"_id": 0, "contact.nom": 1, "contact.poste": 1, "contact.adresse": 1})

    contacts = []

    for university in universities:
        for contact in university.get("contact", []):
            contact_info = {
                "nom": contact.get("nom") if contact.get("nom") != "" else None,
                "poste": contact.get("poste") if contact.get("poste") != "" else None,
                "adresse": contact.get("adresse") if contact.get("adresse") != "" else None
            }
            contacts.append(contact_info)

    client.close()
    return contacts


def generate_contact_code(index):
    return f"contact{str(index).zfill(4)}"

def insert_or_update_contact(contact, index):
    conn = get_postgresql_connection()
    cursor = conn.cursor()

    contact_code = generate_contact_code(index)

    adresse = contact["adresse"]
    if adresse:
        adresse = adresse.strip('{}')

    if not contact["nom"] and not contact["poste"] and not adresse:
        print(f"Skipping insertion for contact: {contact_code}, all fields are NULL")
        return

    query = """
    INSERT INTO dim_contact (contactcode, nom, poste, adresse)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (contactcode) DO UPDATE
    SET nom = EXCLUDED.nom,
        poste = EXCLUDED.poste,
        adresse = EXCLUDED.adresse;
    """
    
    cursor.execute(query, (contact_code, contact["nom"], contact["poste"], adresse))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    contacts = extract_contacts_from_mongodb()
    print("Contacts extraits de MongoDB avec leurs informations générées:")
    
    for index, contact in enumerate(contacts, start=1): 
        print(f"Insertion/Modification du contact : {contact['nom']} - {generate_contact_code(index)}")
        insert_or_update_contact(contact, index)

if __name__ == "__main__":
    main()
