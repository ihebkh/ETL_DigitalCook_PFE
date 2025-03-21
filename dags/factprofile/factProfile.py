import psycopg2
from pymongo import MongoClient

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user='postgres',
        password='admin',
        host='localhost',
        port='5432'
    )

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return collection

def generate_factcode(counter):
    return f"fact{counter:04d}"

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

def get_etude_pk_from_postgres(niveau_etude):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    niveau_etude = niveau_etude.strip().lower()
    
    cur.execute("""
        SELECT niveau_pk 
        FROM public.dim_niveau_d_etudes 
        WHERE lower(label) = %s;
    """, (niveau_etude,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

def get_projet_pk_from_postgres(nom_projet, entreprise, year_start, year_end, month_start, month_end):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT projet_pk 
        FROM public.dim_projet 
        WHERE nom_projet = %s
    """, (nom_projet,)) 
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None


def get_contact_pk_from_postgres(firstname, lastname, company):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT contact_pk 
        FROM public.dim_professional_contact 
        WHERE firstname = %s AND lastname = %s  AND company = %s;
    """, (firstname, lastname, company))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

def get_competence_fk_from_postgres(competence_name):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    competence_name = competence_name.strip().lower()
    
    cur.execute("""
        SELECT competence_pk 
        FROM public.dim_competence_generale 
        WHERE competence_name = %s;
    """, (competence_name,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

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
        return result[0]  
    else:
        return None  

def get_interest_pk_from_postgres(interest_name):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    interest_name = interest_name.strip().lower()
    
    cur.execute("""
        SELECT interests_pk 
        FROM public.dim_interests 
        WHERE interests = %s;
    """, (interest_name,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

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
        return result[0]  
    else:
        return None  
def get_permis_fk_from_postgres(permis_code):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT permis_pk
        FROM public.dim_permis_conduire
        WHERE permis_code = %s;
    """, (permis_code,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]
    else:
        return None


def get_visa_pk_from_postgres(visa_type):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    visa_type = visa_type.strip().lower()
    
    cur.execute("""
        SELECT visa_pk 
        FROM public.dim_visa 
        WHERE lower(visa_type) = %s;
    """, (visa_type,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

def get_certification_pk_from_postgres(certification_name):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT certification_pk
        FROM public.dim_certification
        WHERE nom = %s;
    """, (certification_name,))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None


def get_experience_fk_from_postgres(role, entreprise, type_contrat):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT experience_pk 
        FROM public.dim_experience 
        WHERE role = %s 
          AND entreprise = %s 
          AND typecontrat = %s ;
    """, (role, entreprise, type_contrat))
    
    result = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if result:
        return result[0]  
    else:
        return None  

def insert_data_to_postgres(client_fk, competencegenerales_fk=None, language_fk=None, interests_fk=None, 
                            preferedjoblocations_fk=None, etude_fk=None, visa_fk=None, projet_fk=None, 
                            contact_fk=None, permis_fk=None, certification_pk=None, experience_fk=None,
                            experience_year=0, experience_month=0):
    conn = get_postgres_connection()
    cur = conn.cursor()

    try:
        cur.execute(""" 
            INSERT INTO public.fact_client_profile (
                client_fk, competencegenerales_fk, language_fk, interests_fk, preferedjoblocations_fk, 
                etude_fk, visa_fk, projet_fk, contact_fk, permis_fk, certification_fk, experience_fk,
                experience_year, experience_month
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (client_fk, competencegenerales_fk, language_fk, interests_fk, preferedjoblocations_fk, 
              etude_fk, visa_fk, projet_fk, contact_fk, permis_fk, certification_pk, experience_fk,
              experience_year, experience_month))

        print(f"Data inserted for client_fk: {client_fk}, experience_year: {experience_year}, experience_month: {experience_month}")

        conn.commit()

    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def match_and_display_factcode_client_competence_interest():
    collection = get_mongodb_connection()
    
    mongo_data = collection.find({}, {
        "_id": 0,
        "matricule": 1,
        "profile.dureeExperience": 1,
        "simpleProfile.dureeExperience": 1,
        "profile.permisConduire": 1,
        "simpleProfile.permisConduire": 1,
        "profile.certifications": 1,
        "simpleProfile.certifications": 1,
        "profile.competenceGenerales": 1,
        "profile.languages": 1,
        "profile.interests": 1,
        "profile.preferedJobLocations": 1,
        "profile.niveauDetudes": 1,
        "profile.visa": 1,
        "profile.projets": 1,
        "profile.proffessionalContacts": 1,
        "simpleProfile.languages": 1,
        "simpleProfile.preferedJobLocations": 1, 
    })

    mongo_data_list = list(mongo_data)

    line_count = 0

    experience_year_dict = {}
    experience_month_dict = {}

    for user in mongo_data_list:
        matricule = user.get("matricule", None)

        # Calculate experience in years and months
        dureeExperience = user.get("profile", {}).get("dureeExperience", {})
        dureeExperience_year = dureeExperience.get("year", 0) if isinstance(dureeExperience, dict) else 0
        dureeExperience_month = dureeExperience.get("month", 0) if isinstance(dureeExperience, dict) else 0

        simpleProfile_dureeExperience = user.get("simpleProfile", {}).get("dureeExperience", {})
        simpleProfile_dureeExperience_year = simpleProfile_dureeExperience.get("year", 0) if isinstance(simpleProfile_dureeExperience, dict) else 0
        simpleProfile_dureeExperience_month = simpleProfile_dureeExperience.get("month", 0) if isinstance(simpleProfile_dureeExperience, dict) else 0

        experience_year_dict[matricule] = dureeExperience_year + simpleProfile_dureeExperience_year
        experience_month_dict[matricule] = dureeExperience_month + simpleProfile_dureeExperience_month

        client_fk = get_client_fk_from_postgres(matricule)

        # Initialize lists for foreign keys
        permis_fk_list, competence_fk_list, language_fk_list = [], [], []
        interest_pk_list, job_location_pk_list, study_level_fk_list = [], [], []
        visa_pk_list, project_pk_list, contact_pk_list = [], [], []
        certification_pk_list, experience_fk_list = [], []

        # Populate foreign key lists
        permisConduire = user.get("profile", {}).get("permisConduire", [])
        simpleProfile_permisConduire = user.get("simpleProfile", {}).get("permisConduire", [])
        competenceGenerales = user.get("profile", {}).get("competenceGenerales", [])
        languages = user.get("profile", {}).get("languages", [])
        simpleProfile_languages = user.get("simpleProfile", {}).get("languages", [])
        interests = user.get("profile", {}).get("interests", [])
        preferedJobLocations = user.get("profile", {}).get("preferedJobLocations", [])
        simpleProfile_preferedJobLocations = user.get("simpleProfile", {}).get("preferedJobLocations", [])
        niveau_etudes = user.get("profile", {}).get("niveauDetudes", [])
        visa = user.get("profile", {}).get("visa", [])
        projets = user.get("profile", {}).get("projets", [])
        professionalContacts = user.get("profile", {}).get("proffessionalContacts", [])
        simpleProfile_experiences = user.get("simpleProfile", {}).get("experiences", [])
        experiences = user.get("profile", {}).get("experiences", [])
        certifications = user.get("profile", {}).get("certifications", [])
        simpleProfile_certifications = user.get("simpleProfile", {}).get("certifications", [])

        # Collect foreign keys
        for permis in permisConduire + simpleProfile_permisConduire:
            permis_fk = get_permis_fk_from_postgres(permis)
            if permis_fk:
                permis_fk_list.append(str(permis_fk))

        for experience in experiences + simpleProfile_experiences:
            if isinstance(experience, dict):
                role = experience.get("role", "").strip()
                entreprise = experience.get("entreprise", "").strip()
                type_contrat = experience.get("typeContrat", {}).get("value", "").strip()
                experience_fk = get_experience_fk_from_postgres(role, entreprise, type_contrat)
                if experience_fk:
                    experience_fk_list.append(str(experience_fk))

        for certification in certifications + simpleProfile_certifications:
            if isinstance(certification, dict):
                certification_name = certification.get("nomCertification", "").strip()
                certification_pk = get_certification_pk_from_postgres(certification_name)
                if certification_pk:
                    certification_pk_list.append(str(certification_pk))
            elif isinstance(certification, str):
                certification_name = certification.strip()
                certification_pk = get_certification_pk_from_postgres(certification_name)
                if certification_pk:
                    certification_pk_list.append(str(certification_pk))

        for competence in competenceGenerales:
            competence_fk = get_competence_fk_from_postgres(competence)
            if competence_fk:
                competence_fk_list.append(str(competence_fk))

        for language in languages + simpleProfile_languages:
            language_label = language.get("label", "").strip() if isinstance(language, dict) else language.strip()
            language_level = language.get("level", "").strip() if isinstance(language, dict) else ""
            language_fk = get_language_fk_from_postgres(language_label, language_level)
            if language_fk:
                language_fk_list.append(str(language_fk))

        for interest in interests:
            interest_pk = get_interest_pk_from_postgres(interest)
            if interest_pk:
                interest_pk_list.append(str(interest_pk))

        for location in preferedJobLocations + simpleProfile_preferedJobLocations:
            pays = location.get("pays", "").strip()
            ville = location.get("ville", "").strip()
            region = location.get("region", "").strip()
            preferedjoblocations_pk = get_preferedjoblocations_pk_from_postgres(pays, ville, region)
            if preferedjoblocations_pk:
                job_location_pk_list.append(str(preferedjoblocations_pk))

        for niveau in niveau_etudes:
            niveau_etude_label = niveau.get("label", "").strip() if isinstance(niveau, dict) else niveau.strip()
            etude_fk = get_etude_pk_from_postgres(niveau_etude_label)
            if etude_fk:
                study_level_fk_list.append(str(etude_fk))

        for projet in projets:
            if isinstance(projet, dict):
                nom_projet = projet.get("nomProjet", "").strip()
                entreprise = projet.get("entreprise", "").strip()
                year_start = projet.get("dateDebut", {}).get("year", 0)
                month_start = projet.get("dateDebut", {}).get("month", 0)
                year_end = projet.get("dateFin", {}).get("year", 0)
                month_end = projet.get("dateFin", {}).get("month", 0)
                projet_pk = get_projet_pk_from_postgres(nom_projet, entreprise, year_start, year_end, month_start, month_end)
                if projet_pk:
                    project_pk_list.append(str(projet_pk))

        for visa_item in visa:
            visa_type = visa_item.get("type", "").strip() if isinstance(visa_item, dict) else visa_item.strip()
            visa_pk = get_visa_pk_from_postgres(visa_type)
            if visa_pk:
                visa_pk_list.append(str(visa_pk))

        for contact in professionalContacts:
            firstname = contact.get("firstName", "").strip() if isinstance(contact, dict) else ""
            lastname = contact.get("lastName", "").strip() if isinstance(contact, dict) else ""
            company = contact.get("company", "").strip() if isinstance(contact, dict) else ""
            contact_pk = get_contact_pk_from_postgres(firstname, lastname, company)
            if contact_pk:
                contact_pk_list.append(str(contact_pk))

        max_length = max(len(permis_fk_list), len(competence_fk_list), len(language_fk_list), 
                         len(interest_pk_list), len(job_location_pk_list), len(study_level_fk_list), 
                         len(visa_pk_list), len(project_pk_list), len(contact_pk_list), 
                         len(certification_pk_list), len(experience_fk_list), 1)  # Ensure at least 1

        for i in range(max_length):
            permis_fk = permis_fk_list[i] if i < len(permis_fk_list) else None
            competence_fk = competence_fk_list[i] if i < len(competence_fk_list) else None
            language_fk = language_fk_list[i] if i < len(language_fk_list) else None
            interest_pk = interest_pk_list[i] if i < len(interest_pk_list) else None
            job_location_pk = job_location_pk_list[i] if i < len(job_location_pk_list) else None
            study_level_fk = study_level_fk_list[i] if i < len(study_level_fk_list) else None
            visa_pk = visa_pk_list[i] if i < len(visa_pk_list) else None
            project_pk = project_pk_list[i] if i < len(project_pk_list) else None
            contact_pk = contact_pk_list[i] if i < len(contact_pk_list) else None
            certification_pk = certification_pk_list[i] if i < len(certification_pk_list) else None
            experience_fk = experience_fk_list[i] if i < len(experience_fk_list) else None
            
            # Retrieve experience years and months
            experience_year = experience_year_dict.get(matricule, 0)
            experience_month = experience_month_dict.get(matricule, 0)

            # Call the insert function
            insert_data_to_postgres(client_fk, competence_fk, language_fk, interest_pk, job_location_pk,
                                    study_level_fk, visa_pk, project_pk, contact_pk, permis_fk, 
                                    certification_pk, experience_fk, experience_year, experience_month)
            
            line_count += 1

    print(f"\nTotal lines: {line_count}")

# Call the function to execute the matching and display
match_and_display_factcode_client_competence_interest()