import psycopg2
from pymongo import MongoClient

# Function to get the PostgreSQL connection
def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user='postgres',
        password='admin',
        host='localhost',
        port='5432'
    )

# Function to get the MongoDB connection
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    MONGO_SECTEUR_COLLECTION = "secteurdactivities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    secteur_collection = mongo_db[MONGO_SECTEUR_COLLECTION]
    return client, mongo_db, collection, secteur_collection

# Function to generate a unique factcode
def generate_factcode(counter):
    return f"fact{counter:04d}"

# Function to retrieve client_fk from PostgreSQL using matricule
def get_client_fk_from_postgres(matricule):
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
        return result[0]  # Return client_fk if found
    else:
        return None  # No client_fk found

# Function to get competence_fk from PostgreSQL based on competence name
def get_competence_fk_from_postgres(competence_name):
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
        return result[0]  # Return competence_fk if found
    else:
        return None  # No competence_fk found

# Function to get language_fk from PostgreSQL based on language label and level
def get_language_fk_from_postgres(language_label, language_level):
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
        return result[0]  # Return language_fk if found
    else:
        return None  # No language_fk found

# Function to get interest_pk from PostgreSQL based on interest name
def get_interest_pk_from_postgres(interest_name):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    interest_name = interest_name.strip().lower()  # Normalize the interest name
    
    cur.execute("""
        SELECT interests_pk 
        FROM public.dim_interests 
        WHERE interests = %s;
    """, (interest_name,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return interest_pk if found
    else:
        return None  # No interest_pk found

# Function to get preferedjoblocations_pk from PostgreSQL based on location
def get_preferedjoblocations_pk_from_postgres(pays, ville, region):
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT preferedjoblocations_pk 
        FROM public.dim_preferedjoblocations 
        WHERE pays = %s AND ville = %s AND region = %s;
    """, (pays, ville, region))

    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        return result[0]  # Return preferedjoblocations_pk if found
    else:
        return None  # No preferedjoblocations_pk found

# Function to insert a new prefered job location into PostgreSQL
def insert_new_preferred_job_location(pays, ville, region):
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO public.dim_preferedjoblocations (pays, ville, region)
        VALUES (%s, %s, %s)
        RETURNING preferedjoblocations_pk;
    """, (pays, ville, region))

    new_pk = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    return new_pk

# Function to process the data and generate the factcode, client_fk, language_fk, competence_fk, interest_pk, and preferedjoblocations_pk
def match_and_display_factcode_client_competence_interest():
    client, mongo_db, collection, secteur_collection = get_mongodb_connection()

    # Retrieve all documents from MongoDB
    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.competenceGenerales": 1, "profile.languages": 1, "profile.interests": 1, "profile.preferedJobLocations": 1, "simpleProfile.languages": 1, "simpleProfile.preferedJobLocations": 1})

    global_factcode_counter = 1  # Initialize a global counter for factcode
    line_count = 0  # Line counter

    # Loop through each document in MongoDB
    for user in mongo_data:
        matricule = user.get("matricule", None)
        competenceGenerales = user.get("profile", {}).get("competenceGenerales", [])
        languages = user.get("profile", {}).get("languages", [])
        interests = user.get("profile", {}).get("interests", [])
        preferedJobLocations = user.get("profile", {}).get("preferedJobLocations", [])
        simpleProfile_languages = user.get("simpleProfile", {}).get("languages", [])
        simpleProfile_preferedJobLocations = user.get("simpleProfile", {}).get("preferedJobLocations", [])

        # Get client_fk from PostgreSQL
        client_fk = get_client_fk_from_postgres(matricule)

        # Proceed if client_fk is found
        if client_fk:
            client_has_experience_or_language_or_interest_or_location = False  # Flag for checking if there is any experience, language, interest, or location

            # Display client_fk
            print(f"Matricule: {matricule}, client_fk: {client_fk}")

            # Process competencies
            if competenceGenerales:
                for competence in competenceGenerales:
                    competence_fk = get_competence_fk_from_postgres(competence)
                    if competence_fk:
                        factcode = generate_factcode(global_factcode_counter)
                        print(f"client_fk: {client_fk}, factcode: {factcode}, competence_fk: {competence_fk}")
                        global_factcode_counter += 1
                        line_count += 1
                        client_has_experience_or_language_or_interest_or_location = True

            # Process languages from both profiles
            if languages:
                for language in languages:
                    language_label = language.get("label", "").strip() if isinstance(language, dict) else language.strip()
                    language_level = language.get("level", "").strip() if isinstance(language, dict) else ""
                    
                    language_fk = get_language_fk_from_postgres(language_label, language_level)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"client_fk: {client_fk}, factcode: {factcode}, language_fk: {language_fk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process languages from simpleProfile
            if simpleProfile_languages:
                for language in simpleProfile_languages:
                    language_label = language.get("label", "").strip() if isinstance(language, dict) else language.strip()
                    language_level = language.get("level", "").strip() if isinstance(language, dict) else ""
                    
                    language_fk = get_language_fk_from_postgres(language_label, language_level)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"client_fk: {client_fk}, factcode: {factcode}, language_fk: {language_fk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process interests
            if interests:
                for interest in interests:
                    interest_pk = get_interest_pk_from_postgres(interest)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"client_fk: {client_fk}, factcode: {factcode}, interest_pk: {interest_pk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process prefered job locations from both profiles
            if preferedJobLocations:
                for location in preferedJobLocations:
                    pays = location.get("pays", "").strip()
                    ville = location.get("ville", "").strip()
                    region = location.get("region", "").strip()

                    preferedjoblocations_pk = get_preferedjoblocations_pk_from_postgres(pays, ville, region)
                    if not preferedjoblocations_pk:
                        preferedjoblocations_pk = insert_new_preferred_job_location(pays, ville, region)

                    factcode = generate_factcode(global_factcode_counter)
                    print(f"client_fk: {client_fk}, factcode: {factcode}, preferedjoblocations_pk: {preferedjoblocations_pk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # If no data found, insert a line with nulls
            if not client_has_experience_or_language_or_interest_or_location:
                factcode = generate_factcode(global_factcode_counter)
                print(f"client_fk: {client_fk}, factcode: {factcode}, competence_fk: null, language_fk: null, interest_pk: null, preferedjoblocations_pk: null")
                global_factcode_counter += 1
                line_count += 1

        else:
            print(f"Matricule {matricule} does not have a client_fk in PostgreSQL.")
            line_count += 1

    print(f"\nTotal lines: {line_count}")

# Execute the function to display the factcode, client_fk, language_fk, competence_fk, interest_pk, and preferedjoblocations_pk
match_and_display_factcode_client_competence_interest()
