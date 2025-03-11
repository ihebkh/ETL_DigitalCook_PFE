import psycopg2
from pymongo import MongoClient

# Connexion à MongoDB
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    MONGO_SECTEUR_COLLECTION = "secteurdactivities"

    # Connexion à MongoDB
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    secteur_collection = mongo_db[MONGO_SECTEUR_COLLECTION]
    return client, mongo_db, collection, secteur_collection

# Connexion à PostgreSQL
def get_postgresql_connection():
    conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return conn

# Fonction pour récupérer le secteur_pk en fonction du romeCode depuis PostgreSQL
def get_secteur_pk_from_postgres(rome_code):
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    
    query = "SELECT secteur_pk FROM public.dim_secteur WHERE romecode_jobs = %s"
    cursor.execute(query, (rome_code,))
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result[0]  # Retourner secteur_pk
    else:
        return None  # Aucun secteur trouvé

# Fonction pour insérer ou mettre à jour l'expérience dans PostgreSQL avec secteur_fk
def insert_or_update_experience(codeexperience, role, entreprise, start_year, start_month, end_year, end_month, pays, secteur_fk, ville, type_contrat):
    # Remplacer les chaînes vides par None (NULL dans PostgreSQL)
    start_year = int(start_year) if start_year and start_year != "" else None
    start_month = int(start_month) if start_month and start_month != "" else None
    end_year = int(end_year) if end_year and end_year != "" else None
    end_month = int(end_month) if end_month and end_month != "" else None

    conn = get_postgresql_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO public.dim_experience 
        (codeexperience, role, entreprise, start_year, start_month, end_year, end_month, pays, fk_secteur, ville, typecontrat)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codeexperience) DO UPDATE SET
            role = EXCLUDED.role,
            entreprise = EXCLUDED.entreprise,
            start_year = EXCLUDED.start_year,
            start_month = EXCLUDED.start_month,
            end_year = EXCLUDED.end_year,
            end_month = EXCLUDED.end_month,
            pays = EXCLUDED.pays,
            fk_secteur = EXCLUDED.fk_secteur,
            ville = EXCLUDED.ville,
            typecontrat = EXCLUDED.typecontrat
        RETURNING codeexperience, xmin;
    """
    
    cursor.execute(query, (codeexperience, role, entreprise, start_year, start_month, end_year, end_month, pays, secteur_fk, ville, type_contrat))
    conn.commit()

    # Optionally, retrieve the inserted or updated row
    updated_row = cursor.fetchone()
    print(f"Inserted/Updated: {updated_row}")

    cursor.close()
    conn.close()

# Extraction des expériences et insertion ou mise à jour dans PostgreSQL
def extract_experiences_and_insert_or_update():
    client, mongo_db, collection, secteur_collection = get_mongodb_connection()
    
    # Recherche des documents dans MongoDB
    documents = collection.find({})  # Récupère tous les documents
    code_index = 1  # Compteur pour générer codeexperience comme 'code0001', 'code0002', etc.

    for doc in documents:
        document_id = doc.get('_id', 'Non spécifié')
        print(f"Document ID: {document_id}")
        
        experiences = doc.get('profile', {}).get('experiences', [])
        
        if not experiences:
            print("Pas d'expérience trouvée.")
        else:
            for experience in experiences:
                if isinstance(experience, dict):
                    role = experience.get('role', 'Non spécifié')
                    entreprise = experience.get('entreprise', 'Non spécifié')
                    du_year = experience.get('du', {}).get('year', '')
                    du_month = experience.get('du', {}).get('month', '')
                    au_year = experience.get('au', {}).get('year', '')
                    au_month = experience.get('au', {}).get('month', '')
                    pays = experience.get('pays', {}).get('value', 'Non spécifié')

                    # Handling ville and typeContrat - extracting 'value' from nested objects
                    ville = experience.get('ville', {}).get('value', 'Non spécifiée') if experience.get('ville') else 'Non spécifiée'
                    type_contrat = experience.get('typeContrat', {}).get('value', 'Non spécifié') if experience.get('typeContrat') else 'Non spécifié'

                    experience_str = f"{role}, {entreprise}, {du_year}, {du_month}, {au_year}, {au_month}, {pays}, {ville}, {type_contrat}"
                    
                    secteur_id = experience.get('secteur')
                    secteur_fk = None  # Initialisation de secteur_fk par défaut
                    
                    if secteur_id:
                        secteur_data = secteur_collection.find_one({"_id": secteur_id})
                        if secteur_data:
                            for job in secteur_data.get('jobs', []):
                                rome_code = job.get('romeCode')
                                print(f"{experience_str}")
                                
                                # Récupérer secteur_pk en fonction du rome_code depuis PostgreSQL
                                secteur_fk = get_secteur_pk_from_postgres(rome_code)
                                
                                if secteur_fk:
                                    print(f"Secteur_pk trouvé: {secteur_fk}")
                                    # Générer codeexperience de type 'code0001', 'code0002', etc.
                                    codeexperience = f"code{str(code_index).zfill(4)}"
                                    code_index += 1
                                    
                                    # Insérer ou mettre à jour les données dans PostgreSQL avec secteur_fk
                                    insert_or_update_experience(
                                        codeexperience,
                                        role,
                                        entreprise,
                                        du_year,
                                        du_month,
                                        au_year,
                                        au_month,
                                        pays,
                                        secteur_fk,
                                        ville,
                                        type_contrat
                                    )
                                else:
                                    print(f"Secteur_pk non trouvé pour le romeCode: {rome_code}")
                        else:
                            print(f"{experience_str}, Secteur non trouvé dans la collection secteurdactivities.")
                    else:
                        print(f"{experience_str}, Aucun secteur spécifié.")
                else:
                    print(f"Expérience mal formatée: {experience}")
                print('-------------------------')

# Appel de la fonction pour afficher les expériences et le secteur_fk dans PostgreSQL
extract_experiences_and_insert_or_update()
