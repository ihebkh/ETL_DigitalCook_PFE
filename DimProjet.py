import psycopg2
from pymongo import MongoClient

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

def get_postgresql_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def safe_int(value):
    try:
        return int(value) if value and isinstance(value, (str, int)) and str(value).isdigit() else None
    except ValueError:
        return None

def get_existing_projects():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT nom_projet, entreprise, code_projet FROM dim_projet")
    existing_projects = {(row[0], row[1]): row[2] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return existing_projects

def generate_project_code(existing_codes):
    if not existing_codes:
        new_number = 1 
    else:
        last_numbers = [int(code[4:]) for code in existing_codes if code.startswith("PROJ") and code[4:].isdigit()]
        new_number = max(last_numbers) + 1 if last_numbers else 1

    return f"PROJ{str(new_number).zfill(2)}" 

def extract_from_mongodb():
    client, _, collection = get_mongodb_connection()
    mongo_data = collection.find({}, {"_id": 0, "profile.projets": 1, "simpleProfile.projets": 1})

    projects = []
    
    for user in mongo_data:
        if isinstance(user, dict) and "profile" in user and isinstance(user["profile"], dict):
            profile = user["profile"]
            if "projets" in profile:
                projets_list = profile["projets"]
                for project in projets_list:
                    if isinstance(project, dict): 
                        projects.append({
                            "nom_projet": project.get("nomProjet"),
                            "year_start": safe_int(project.get("dateDebut", {}).get("year", "")),
                            "month_start": safe_int(project.get("dateDebut", {}).get("month", "")),
                            "year_end": safe_int(project.get("dateFin", {}).get("year", "")),
                            "month_end": safe_int(project.get("dateFin", {}).get("month", "")),
                            "entreprise": project.get("entreprise"),
                            "code_projet": None 
                        })
        
        if isinstance(user, dict) and "simpleProfile" in user and isinstance(user["simpleProfile"], dict):
            simpleProfile = user["simpleProfile"]
            if "projets" in simpleProfile:
                projets_list = simpleProfile["projets"]
                for project in projets_list:
                    if isinstance(project, dict): 
                        projects.append({
                            "nom_projet": project.get("nomProjet"),
                            "year_start": safe_int(project.get("dateDebut", {}).get("year", "")),
                            "month_start": safe_int(project.get("dateDebut", {}).get("month", "")),
                            "year_end": safe_int(project.get("dateFin", {}).get("year", "")),
                            "month_end": safe_int(project.get("dateFin", {}).get("month", "")),
                            "entreprise": project.get("entreprise"),
                            "code_projet": None 
                        })

    client.close()
    
    print("Projets extraits :", projects)  
    return projects

def transform_data(mongo_data, existing_projects):
    unique_projects = {}

    for record in mongo_data:
        key = (record["nom_projet"], record["entreprise"])
        
        if key in existing_projects:
            record["code_projet"] = existing_projects[key] 
        else:
            new_code = generate_project_code(existing_projects.values()) 
            existing_projects[key] = new_code
            record["code_projet"] = new_code

        unique_projects[key] = record

    return list(unique_projects.values())

def load_into_postgres(data):
    conn = get_postgresql_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_projet (code_projet, nom_projet, year_start, month_start, year_end, month_end, entreprise)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (code_projet) DO UPDATE SET
        nom_projet = EXCLUDED.nom_projet,
        year_start = EXCLUDED.year_start,
        month_start = EXCLUDED.month_start,
        year_end = EXCLUDED.year_end,
        month_end = EXCLUDED.month_end,
        entreprise = EXCLUDED.entreprise
    """

    for record in data:
        values = (
            record["code_projet"],
            record["nom_projet"],
            record["year_start"],
            record["month_start"],
            record["year_end"],
            record["month_end"],
            record["entreprise"],
        )
        print(f"Insertion / Mise à jour : {values}")
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("--- Extraction et chargement des projets ---")
    
    raw_data = extract_from_mongodb()
    existing_projects = get_existing_projects()
    transformed_data = transform_data(raw_data, existing_projects)
    
    if transformed_data:
        load_into_postgres(transformed_data)
        print("Données insérées/mises à jour avec succès dans PostgreSQL.")
    else:
        print("Aucune donnée à insérer.")

if __name__ == "__main__":
    main()
