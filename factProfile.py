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

# Function to generate a factcode dynamically (e.g., fact0001, fact0002, ...)
def generate_factcode(counter):
    return f"fact{counter:04d}"  # Generates fact0001, fact0002, ...

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

# Function to fetch competence_pk from PostgreSQL based on competence name
def get_competence_pk_from_postgres(competence_name):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    competence_name = competence_name.strip().lower()  # Normalize the competence name
    
    cur.execute("""
        SELECT competence_pk 
        FROM public.dim_competence_generale 
        WHERE competence_name = %s;
    """, (competence_name,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return the competence_pk if found
    else:
        return None  # No matching competence_pk found

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

# Function to insert factcode and client_fk into fact_client_profile
def insert_factcode_and_client(client_pk, factcode):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    # Insert factcode and client_fk into fact_client_profile
    cur.execute("""
        INSERT INTO public.fact_client_profile (client_fk, factcode) 
        VALUES (%s, %s);
    """, (client_pk, factcode))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted factcode: {factcode}, client_pk: {client_pk}")

# Function to update competence_fk for a given factcode
def update_competence_fk(factcode, competence_pk):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    # Update competence_fk based on the factcode
    cur.execute("""
        UPDATE public.fact_client_profile 
        SET competencegenerales_fk = %s 
        WHERE factcode = %s;
    """, (competence_pk, factcode))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Updated competence_fk: {competence_pk} for factcode: {factcode}")

# Function to update language_fk for a given factcode
def update_language_fk(factcode, language_pk):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    # Update language_fk based on the factcode
    cur.execute("""
        UPDATE public.fact_client_profile 
        SET language_fk = %s 
        WHERE factcode = %s;
    """, (language_pk, factcode))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Updated language_fk: {language_pk} for factcode: {factcode}")

# Function to extract matricule, client_pk, competenceGenerales, and languages from MongoDB and insert into PostgreSQL
def extract_matricule_and_competencies_with_pk():
    client, _, collection = get_mongodb_connection()
    
    # Fetch the documents containing matricule, competenceGenerales, and languages from MongoDB
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.competenceGenerales": 1, "profile.languages": 1, "simpleProfile.languages": 1})

    factcode_counter = 1  # Initialize factcode counter
    
    # Iterate through each document
    for user in mongo_data:
        matricule = user.get("matricule", None)
        client_pk = get_client_pk_from_postgres(matricule)  # Fetch client_pk from PostgreSQL based on matricule
        competenceGenerales = user.get("profile", {}).get("competenceGenerales", [])
        profile_languages = user.get("profile", {}).get("languages", [])
        simple_profile_languages = user.get("simpleProfile", {}).get("languages", [])

        # Handle the case where matricule is None (if no client_pk found)
        if not client_pk:
            print(f"Matricule {matricule} not found or is null in PostgreSQL. Inserting as null client_pk.")
            client_pk = None  # Assign None for client_pk if not found

        # Process competencies and languages
        if client_pk is not None:  # If client_pk is found or not null
            # Generate factcode and insert factcode and client_fk
            factcode = generate_factcode(factcode_counter)
            insert_factcode_and_client(client_pk, factcode)
            factcode_counter += 1  # Increment the counter
            # Process competencies
            if competenceGenerales:
                for competence in competenceGenerales:
                    # Fetch competence_pk from PostgreSQL for each competence
                    competence_pk = get_competence_pk_from_postgres(competence)
                    update_competence_fk(factcode, competence_pk)  # Update competence_fk for the factcode
                    
            # Process languages from profile
            if profile_languages:
                for lang in profile_languages:
                    language_label = lang.get("label", "").strip() if isinstance(lang, dict) else lang.strip()
                    language_level = lang.get("level", "").strip() if isinstance(lang, dict) else ""
                    
                    language_pk = get_language_pk_from_postgres(language_label, language_level)
                    update_language_fk(factcode, language_pk)  # Update language_fk for the factcode
                    
            # Process languages from simpleProfile (if any)
            if simple_profile_languages:
                for lang in simple_profile_languages:
                    language_label = lang.get("label", "").strip() if isinstance(lang, dict) else lang.strip()
                    language_level = lang.get("level", "").strip() if isinstance(lang, dict) else ""
                    
                    language_pk = get_language_pk_from_postgres(language_label, language_level)
                    update_language_fk(factcode, language_pk)  # Update language_fk for the factcode
        else:
            # If matricule is null or not found, still insert as null client_pk
            factcode = generate_factcode(factcode_counter)
            insert_factcode_and_client(None, factcode)
            factcode_counter += 1  # Increment the counter
        
        print("-" * 50)  # Separator for readability

    print("All data extracted and inserted into PostgreSQL.")

# Run the function to extract data and insert into PostgreSQL
extract_matricule_and_competencies_with_pk()
