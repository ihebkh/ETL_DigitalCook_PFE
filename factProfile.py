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
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return collection

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
        return result[0]  
    else:
        return None  

# Function to retrieve etude_pk from PostgreSQL based on study name (niveau d'étude)
def get_etude_pk_from_postgres(niveau_etude):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    niveau_etude = niveau_etude.strip().lower()  # Normalize the level of study name
    
    cur.execute("""
        SELECT niveau_pk 
        FROM public.dim_niveau_d_etudes 
        WHERE lower(label) = %s;
    """, (niveau_etude,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return etude_pk if found
    else:
        return None  # No etude_pk found

# Function to retrieve projet_pk from PostgreSQL based on project name
def get_projet_pk_from_postgres(nom_projet, entreprise, year_start, year_end, month_start, month_end):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT projet_pk 
        FROM public.dim_projet 
        WHERE nom_projet = %s AND entreprise = %s
    """, (nom_projet, entreprise))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return projet_pk if found
    else:
        return None  # No projet_pk found

# Function to retrieve contact_pk from PostgreSQL based on professional contact details
def get_contact_pk_from_postgres(firstname, lastname, email, company):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT contact_pk 
        FROM public.dim_professional_contact 
        WHERE firstname = %s AND lastname = %s AND email = %s AND company = %s;
    """, (firstname, lastname, email, company))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return contact_pk if found
    else:
        return None  # No contact_pk found

# Function to retrieve competence_fk from PostgreSQL based on competence name
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

# Function to retrieve language_fk from PostgreSQL based on language label and level
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

# Function to retrieve interest_pk from PostgreSQL based on interest name
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

# Function to retrieve preferedjoblocations_pk from PostgreSQL based on location
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
    
# Function to retrieve visa_pk from PostgreSQL based on visa type
def get_visa_pk_from_postgres(visa_type):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    visa_type = visa_type.strip().lower()  # Normalize the visa type
    
    cur.execute("""
        SELECT visa_pk 
        FROM public.dim_visa 
        WHERE lower(visa_type) = %s;
    """, (visa_type,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return visa_pk if found
    else:
        return None  # No visa_pk found

def get_certification_pk_from_postgres(certification_name, year, month):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT certification_pk
        FROM public.dim_certification
        WHERE nom = %s AND year = %s AND month = %s;
    """, (certification_name, year, month))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  # Return certification_pk if found
    else:
        return None  # No certification_pk found

def match_and_display_factcode_client_competence_interest():
    collection = get_mongodb_connection()

    mongo_data = collection.find({}, {"_id": 0, "matricule": 1, "profile.certifications": 1, "simpleProfile.certifications": 1 , "profile.competenceGenerales": 1, "profile.languages": 1, "profile.interests": 1, "profile.preferedJobLocations": 1, "profile.niveauDetudes": 1, "profile.visa": 1, "profile.projets": 1, "profile.professionalContacts": 1, "simpleProfile.languages": 1, "simpleProfile.preferedJobLocations": 1})

    global_factcode_counter = 1  
    line_count = 0  

    for user in mongo_data:
        matricule = user.get("matricule", None)
        competenceGenerales = user.get("profile", {}).get("competenceGenerales", [])
        languages = user.get("profile", {}).get("languages", [])
        interests = user.get("profile", {}).get("interests", [])
        preferedJobLocations = user.get("profile", {}).get("preferedJobLocations", [])
        niveau_etudes = user.get("profile", {}).get("niveauDetudes", [])
        visa = user.get("profile", {}).get("visa", [])
        projets = user.get("profile", {}).get("projets", [])
        professionalContacts = user.get("profile", {}).get("professionalContacts", [])
        simpleProfile_languages = user.get("simpleProfile", {}).get("languages", [])
        simpleProfile_preferedJobLocations = user.get("simpleProfile", {}).get("preferedJobLocations", [])

        client_fk = get_client_fk_from_postgres(matricule)

        if client_fk:
            client_has_experience_or_language_or_interest_or_location = False  

            print(f"Matricule: {matricule}-----------------------------------------------------------------------")

            # Process competencies from both profiles
            if competenceGenerales:
                for competence in competenceGenerales:
                    competence_fk = get_competence_fk_from_postgres(competence)
                    if competence_fk:
                        factcode = generate_factcode(global_factcode_counter)
                        print(f"Competence - client_fk: {client_fk}, factcode: {factcode}, competence_fk: {competence_fk}")
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
                    print(f"Language - client_fk: {client_fk}, factcode: {factcode}, language_fk: {language_fk}")
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
                    print(f"SimpleProfile Language - client_fk: {client_fk}, factcode: {factcode}, language_fk: {language_fk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process interests
            if interests:
                for interest in interests:
                    interest_pk = get_interest_pk_from_postgres(interest)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"Interest - client_fk: {client_fk}, factcode: {factcode}, interest_pk: {interest_pk}")
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

                    factcode = generate_factcode(global_factcode_counter)
                    print(f"Job Location - client_fk: {client_fk}, factcode: {factcode}, preferedjoblocations_pk: {preferedjoblocations_pk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process niveau d'études
            if niveau_etudes:
                for niveau in niveau_etudes:
                    niveau_etude_label = niveau.get("label", "").strip() if isinstance(niveau, dict) else niveau.strip()
                    etude_fk = get_etude_pk_from_postgres(niveau_etude_label)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"Education Level - client_fk: {client_fk}, factcode: {factcode}, etude_fk: {etude_fk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process projects
            if projets:
                for projet in projets:
                    if isinstance(projet, dict):
                        nom_projet = projet.get("nomProjet", "").strip()
                        entreprise = projet.get("entreprise", "").strip()
                        year_start = projet.get("dateDebut", {}).get("year", 0)
                        month_start = projet.get("dateDebut", {}).get("month", 0)
                        year_end = projet.get("dateFin", {}).get("year", 0)
                        month_end = projet.get("dateFin", {}).get("month", 0)

                        projet_pk = get_projet_pk_from_postgres(nom_projet, entreprise, year_start, year_end, month_start, month_end)

                        factcode = generate_factcode(global_factcode_counter)
                        print(f"Project - client_fk: {client_fk}, factcode: {factcode}, projet_pk: {projet_pk}")
                        global_factcode_counter += 1
                        line_count += 1
                        client_has_experience_or_language_or_interest_or_location = True

            # Process visas
            if visa:
                for visa_item in visa:
                    visa_type = visa_item.get("type", "").strip() if isinstance(visa_item, dict) else visa_item.strip()
                    visa_pk = get_visa_pk_from_postgres(visa_type)
                    factcode = generate_factcode(global_factcode_counter)
                    print(f"Visa - client_fk: {client_fk}, factcode: {factcode}, visa_pk: {visa_pk}")
                    global_factcode_counter += 1
                    line_count += 1
                    client_has_experience_or_language_or_interest_or_location = True

            # Process professional contacts
            if professionalContacts:
                for contact in professionalContacts:
                    # Initialize variables to prevent UnboundLocalError
                    firstname = contact.get("firstName", "").strip() if isinstance(contact, dict) else ""
                    lastname = contact.get("lastName", "").strip() if isinstance(contact, dict) else ""
                    email = contact.get("email", "").strip() if isinstance(contact, dict) else ""
                    company = contact.get("company", "").strip() if isinstance(contact, dict) else ""

                    contact_pk = get_contact_pk_from_postgres(firstname, lastname, email, company)
                    if contact_pk:
                        print(f"Professional Contact - client_fk: {client_fk}, contact_pk: {contact_pk}, firstName: {firstname}, lastName: {lastname}, email: {email}, company: {company}")
                    else:
                        print(f"Professional Contact not found in PostgreSQL - client_fk: {client_fk}, firstName: {firstname}, lastName: {lastname}, email: {email}, company: {company}")

            # If no data found, insert a line with nulls
            if not client_has_experience_or_language_or_interest_or_location:
                print(f"client_fk: {client_fk}, factcode: {factcode}, competence_fk: null, language_fk: null, interest_pk: null, preferedjoblocations_pk: null, etude_fk: null, visa_pk: null, projet_pk: null, contact_pk: null")
                global_factcode_counter += 1
                line_count += 1

        else:
            print(f"Matricule {matricule} does not have a client_fk in PostgreSQL.")
            line_count += 1

    print(f"\nTotal lines: {line_count}")

# Execute the function to display the factcode, client_fk, language_fk, competence_fk, interest_pk, preferedjoblocations_pk, etude_fk, visa_pk, projet_pk
match_and_display_factcode_client_competence_interest()
