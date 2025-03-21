import psycopg2
from pymongo import MongoClient

def get_postgresql_connection():
    conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return conn

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
        return None

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

def generate_experience_code(existing_codes):
    if not existing_codes:
        return "expr0001"
    
    filtered_codes = [code for code in existing_codes if code.startswith("expr")]
    
    if not filtered_codes:
        return "expr0001"
    
    last_number = max(int(code.replace("expr", "")) for code in filtered_codes)
    new_number = last_number + 1
    return f"expr{str(new_number).zfill(4)}"  # Ensure the code is always 4 digits

def check_experience_code_exists(codeexperience):
    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = "SELECT 1 FROM public.dim_experience WHERE codeexperience = %s"
    cursor.execute(query, (codeexperience,))
    
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    return result is not None  # Returns True if the code already exists

def insert_or_update_experience(data):
    # Ensure that the codeexperience does not exist before inserting
    codeexperience = data[0]

    if check_experience_code_exists(codeexperience):
        # If the codeexperience already exists, generate a new one
        while check_experience_code_exists(codeexperience):
            codeexperience = generate_experience_code([codeexperience])  # Generate a new unique code
        
        # Update the data with the new code
        data = (codeexperience, *data[1:])

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO public.dim_experience (codeexperience, role, entreprise, start_year, start_month, end_year, end_month, pays, ville, typecontrat, fk_secteur)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (codeexperience) DO UPDATE
    SET role = EXCLUDED.role,
        entreprise = EXCLUDED.entreprise,
        start_year = EXCLUDED.start_year,
        start_month = EXCLUDED.start_month,
        end_year = EXCLUDED.end_year,
        end_month = EXCLUDED.end_month,
        pays = EXCLUDED.pays,
        ville = EXCLUDED.ville,
        typecontrat = EXCLUDED.typecontrat,
        fk_secteur = EXCLUDED.fk_secteur;
    """
    
    cursor.execute(query, data)
    conn.commit()
    cursor.close()
    conn.close()

def extract_profile_data():
    client, mongo_db, collection, secteur_collection = get_mongodb_connection()

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = "SELECT codeexperience FROM public.dim_experience"
    cursor.execute(query)
    existing_codes = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    profiles = collection.find()

    for profile in profiles:
        profile_data = profile.get('profile') or profile.get('simpleprofile')
        if not profile_data:
            continue

        experiences = profile_data.get('experiences', [])
        for experience in experiences:
            if isinstance(experience, dict):
                role = experience.get('role', 'No role')
                entreprise = experience.get('entreprise', 'No entreprise')

                du_year = experience.get('du', {}).get('year')
                du_month = experience.get('du', {}).get('month')
                au_year = experience.get('au', {}).get('year')
                au_month = experience.get('au', {}).get('month')

                du_year = int(du_year) if du_year.isdigit() else 0
                du_month = int(du_month) if du_month.isdigit() else 1
                au_year = int(au_year) if au_year.isdigit() else 0
                au_month = int(au_month) if au_month.isdigit() else 1

                pays = experience.get('pays', {}).get('value', 'No country')
                ville = experience.get('ville', {}).get('value', 'No city')
                typecontrat = experience.get('typecontrat', 'No contract type')
                experience_secteur = experience.get('secteur')
            else:
                experience_secteur = experience

            secteur_doc_experience = secteur_collection.find_one({"_id": experience_secteur})
            if secteur_doc_experience:
                jobs = secteur_doc_experience.get('jobs', [])
                
                for job in jobs:
                    romeCode = job.get('romeCode', 'No romeCode')
                    metier_pk = get_metier_pk_from_rome_code(romeCode)

                    # Generate a new unique experience code for each entry
                    codeexperience = generate_experience_code(existing_codes)

                    data = (
                        codeexperience, 
                        role, entreprise, du_year, du_month, au_year, au_month, pays, ville, typecontrat, metier_pk
                    )

                    print(data)
                    insert_or_update_experience(data)

                # After inserting, update the existing_codes list with new codes from the database
                conn = get_postgresql_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                existing_codes = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()

            else:
                print(f"No matching secteur found in secteurdactivities collection for experience secteur: {experience_secteur}")
            print('---')

if __name__ == "__main__":
    extract_profile_data()
