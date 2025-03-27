import logging
from pymongo import MongoClient
import psycopg2

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    """Établit la connexion à MongoDB et retourne le client et les collections nécessaires."""
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        
        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        
        # Collections à utiliser
        universities_collection = mongo_db["universities"]
        frontusers_collection = mongo_db["frontusers"]
        centrefinancements_collection = mongo_db["centrefinancements"]
        
        logger.info("MongoDB connection successful.")
        
        return client, universities_collection, frontusers_collection, centrefinancements_collection
    
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def extract_and_process_data():
    """Extrait les données des collections MongoDB et les transforme."""
    try:
        client, universities_collection, frontusers_collection, centrefinancements_collection = get_mongodb_connection()

        contacts = []

        # Extraction des contacts des universités
        universities = universities_collection.find({}, {"_id": 0, "nom": 1, "contact.nom": 1, "contact.poste": 1, "contact.adresse": 1, "contact.company": 1, "contact.lastname": 1})
        for university in universities:
            for contact in university.get("contact", []):
                contact_info = {
                    "firstname": contact.get("nom") or None,
                    "lastname": contact.get("lastname") or None,
                    "poste": contact.get("poste") or None,
                    "adresse": contact.get("adresse") or None,
                    "company": contact.get("company") or None,
                    "typecontact": "Université"  # Type de contact universitaire
                }
                contacts.append(contact_info)

        # Extraction des contacts professionnels des frontusers
        existing_entries = set()
        frontusers_data = frontusers_collection.find({}, {"_id": 0, "profile.proffessionalContacts": 1})
        for user in frontusers_data:
            if "profile" in user and "proffessionalContacts" in user["profile"]:
                for contact in user["profile"]["proffessionalContacts"]:
                    contact_key = (contact.get("firstName"), contact.get("lastName"), contact.get("company"))
                    if contact_key not in existing_entries:
                        contacts.append({
                            "firstname": contact.get("firstName"),
                            "lastname": contact.get("lastName"),
                            "company": contact.get("company"),
                            "poste": contact.get("poste", "null"),
                            "adresse": contact.get("adresse", "null"),
                            "typecontact": "Professionnel"  # Type de contact professionnel
                        })
                        existing_entries.add(contact_key)

        # Extraction des données de la collection centrefinancements
        centrefinancements_data = centrefinancements_collection.find({}, {
            "_id": 0, "contactPersonel.nom": 1, "contactPersonel.poste": 1
        })
        for record in centrefinancements_data:
            for contact in record.get("contactPersonel", []):
                contact_info = {
                    "nom": contact.get("nom", None),
                    "poste": contact.get("poste", None),
                    "lastname": None,  # Valeur par défaut "null" pour lastname
                    "company": None,  # Valeur par défaut "null" pour company
                    "adresse": None,  # Valeur par défaut "null" pour adresse
                    "typecontact": "Personnel"  # Type par défaut "Personnel"
                }
                contacts.append(contact_info)

        client.close()

        return contacts
    
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def get_postgresql_connection():
    """Établit la connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname="DW_DigitalCook", 
            user="postgres", 
            password="admin", 
            host="localhost", 
            port="5432"
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def insert_or_update_data_into_postgresql(contacts):
    """Insérer ou mettre à jour les contacts extraits dans PostgreSQL."""
    try:
        # Connexion à PostgreSQL
        conn = get_postgresql_connection()
        cursor = conn.cursor()
        
        # Requête d'insertion ou mise à jour avec ON CONFLICT
        insert_or_update_query = """
        INSERT INTO public.dim_contact (contact_code, firstname, lastname, company, typecontact, poste, adresse)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (contact_code) 
        DO UPDATE 
        SET firstname = EXCLUDED.firstname, 
            lastname = EXCLUDED.lastname,
            company = EXCLUDED.company,
            typecontact = EXCLUDED.typecontact,
            poste = EXCLUDED.poste,
            adresse = EXCLUDED.adresse;
        """
        
        # Initialiser un compteur pour le contact_code
        contact_code_counter = 1
        
        # Insérer ou mettre à jour chaque contact dans la table PostgreSQL
        for contact in contacts:
            contact_code = generate_contact_code(contact_code_counter)
            contact_code_counter += 1  # Incrémentation du contact_code
            values = (
                contact_code,
                contact.get("firstname"),
                contact.get("lastname"),
                contact.get("company"),
                contact.get("typecontact"),  # Utilisation du type de contact
                contact.get("poste"),
                contact.get("adresse")
            )
            cursor.execute(insert_or_update_query, values)
        
        # Commit les changements dans la base de données
        conn.commit()
        logger.info(f"Successfully inserted/updated {len(contacts)} contacts in PostgreSQL.")
        
        # Fermeture de la connexion
        cursor.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"Error inserting/updating data into PostgreSQL: {e}")
        raise

def generate_contact_code(index):
    """Générer un code unique pour chaque contact, du format contact0001, contact0002, etc."""
    return f"contact{str(index).zfill(4)}"

# Intégration du tout : extraction et insertion
def main():
    # Extraction des données de MongoDB
    contacts = extract_and_process_data()

    # Insertion ou mise à jour des contacts dans PostgreSQL
    insert_or_update_data_into_postgresql(contacts)

# Exécuter le processus complet
if __name__ == "__main__":
    main()
