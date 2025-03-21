import psycopg2
from pymongo import MongoClient
import re

def get_postgresql_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def get_metier_pk_from_rome_code(rome_code):
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    
    query = "SELECT metier_pk FROM public.dim_metier WHERE romeCode = %s"
    cursor.execute(query, (rome_code,))
    
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()

    if result:
        return result[0]
    else:
        return 'No metier_pk found'

def generate_codeexperience(existing_codes):
    valid_codes = [code for code in existing_codes if re.match(r"^exper\d{4}$", code)]
    
    if not valid_codes:
        return "exper0001"
    else:
        last_number = max(int(code.replace("exper", "")) for code in valid_codes)
        new_number = last_number + 1
        return f"exper{str(new_number).zfill(4)}"

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    SECTEUR_COLLECTION = "secteurdactivities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    secteur_collection = mongo_db[SECTEUR_COLLECTION]
    return client, mongo_db, collection, secteur_collection

def extract_from_mongodb():
    client, _, collection, _ = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.experiences": 1})

    extracted_data = []

    for user in mongo_data:
        if 'profile' in user:
            profile_data = user['profile']
            if 'experiences' in profile_data:
                for experience in profile_data['experiences']:
                    # Vérifier si experience est un dictionnaire
                    if isinstance(experience, dict):
                        data = {
                            'role': experience.get('role', 'No role'),
                            'entreprise': experience.get('entreprise', 'No entreprise'),
                            'start_year': experience.get('du', {}).get('year', None),
                            'start_month': experience.get('du', {}).get('month', None),
                            'end_year': experience.get('au', {}).get('year', None),
                            'end_month': experience.get('au', {}).get('month', None),
                            'pays': experience.get('pays', {}).get('value', 'No country'),
                            'ville': experience.get('ville', {}).get('value', 'No city'),
                            'typecontrat': experience.get('typecontrat', 'No contract type'),
                            'secteur': experience.get('secteur')
                        }
                        extracted_data.append(data)
                    else:
                        print(f"Expérience invalide (attendu dictionnaire, trouvé {type(experience)}): {experience}")
    
    client.close()
    return extracted_data


def load_into_postgres(data):
    conn = get_postgresql_connection()
    cur = conn.cursor()

    cur.execute("SELECT codeexperience FROM public.dim_experience")
    existing_codes = {row[0] for row in cur.fetchall()}

    insert_query = """
    INSERT INTO public.dim_experience (
        codeexperience, role, entreprise, start_year, start_month, end_year, 
        end_month, pays, ville, typecontrat, fk_secteur
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (codeexperience) DO NOTHING;
    """

    update_query = """
    UPDATE public.dim_experience
    SET role = %s, entreprise = %s, start_year = %s, start_month = %s, end_year = %s, 
        end_month = %s, pays = %s, ville = %s, typecontrat = %s, fk_secteur = %s
    WHERE codeexperience = %s;
    """

    for record in data:
        codeexperience = generate_codeexperience(existing_codes)
        existing_codes.add(codeexperience)

        # Check if record exists
        cur.execute("SELECT 1 FROM public.dim_experience WHERE codeexperience = %s", (codeexperience,))
        if cur.fetchone():
            print(f"Updating experience with code: {codeexperience}")
            cur.execute(update_query, (record['role'], record['entreprise'], record['start_year'], 
                                       record['start_month'], record['end_year'], record['end_month'],
                                       record['pays'], record['ville'], record['typecontrat'], 
                                       record['secteur'], codeexperience))
        else:
            print(f"Inserting experience with code: {codeexperience}")
            cur.execute(insert_query, (codeexperience, record['role'], record['entreprise'], record['start_year'], 
                                       record['start_month'], record['end_year'], record['end_month'],
                                       record['pays'], record['ville'], record['typecontrat'], 
                                       record['secteur']))

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des expériences ---")
    raw_data = extract_from_mongodb()

    if raw_data:
        load_into_postgres(raw_data)
        print("Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
