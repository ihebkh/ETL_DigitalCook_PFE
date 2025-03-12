import psycopg2
from pymongo import MongoClient

# Function to get MongoDB connection
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

# Function to get PostgreSQL connection
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user='postgres',
        password='admin',
        host='localhost',
        port='5432'
    )

# Function to fetch client_pk from PostgreSQL based on matricule
def get_client_pk_from_postgres(matricule):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT client_pk 
        FROM public.dim_client 
        WHERE matricule = %s;
    """, (str(matricule),))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return the client_pk if found
    else:
        return None  # No matching client_pk found

# Function to fetch language_pk from PostgreSQL based on language label and level
def get_language_pk_from_postgres(language_label, language_level):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT langue_pk 
        FROM public.dim_languages 
        WHERE label = %s AND level = %s;
    """, (language_label, language_level))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return the language code if found
    else:
        return None  # No matching language_pk found

# Function to process and verify clients and languages in documents
def process_and_verify_data():
    with open("output.txt", "w") as file:
        client, _, collection = get_mongodb_connection()
        records = collection.find({}, {"_id": 1, "matricule": 1, "profile.languages": 1, "simpleProfile.languages": 1})  # Get data from MongoDB
        
        for record in records:
            matricule = record.get("matricule")
            client_pk = get_client_pk_from_postgres(matricule) if matricule else None
            profile_languages = record.get('profile', {}).get('languages', [])
            simple_profile_languages = record.get('simpleProfile', {}).get('languages', [])

            # Process all languages from profile
            if profile_languages:
                for lang in profile_languages:
                    language_label = lang.get("label", "").strip() if isinstance(lang, dict) else lang.strip()
                    language_level = lang.get("level", "").strip() if isinstance(lang, dict) else ""
                    language_pk = get_language_pk_from_postgres(language_label, language_level)
                    if language_pk:
                        file.write(f"client pk : {client_pk if client_pk else 'null'}, language_pk {language_pk}\n")

            # Process all languages from simpleProfile (if any)
            if simple_profile_languages:
                for lang in simple_profile_languages:
                    language_label = lang.get("label", "").strip() if isinstance(lang, dict) else lang.strip()
                    language_level = lang.get("level", "").strip() if isinstance(lang, dict) else ""
                    language_pk = get_language_pk_from_postgres(language_label, language_level)
                    if language_pk:
                        file.write(f"client pk : {client_pk if client_pk else 'null'}, language_pk {language_pk}\n")

    print("All data processed and saved to output.txt.")

# Run the process
process_and_verify_data()
