from pymongo import MongoClient
from bson import ObjectId
import psycopg2
from bson.errors import InvalidId

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
    secteur_collection = mongo_db["secteurdactivities"]
    
    return collection, secteur_collection

def safe_object_id(id_str):
    try:
        return ObjectId(id_str)
    except InvalidId:
        return None

def generate_factcode(counter):
    return f"fact{counter:04d}"

def load_dim_secteur():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT secteur_pk, label FROM public.dim_secteur;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {label.lower(): pk for pk, label in rows if label}

def load_dim_metier():
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("SELECT metier_pk, label_jobs FROM public.dim_metier;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {label.lower(): pk for pk, label in rows if label}

def get_visa_counts_by_client():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT f.client_fk, COUNT(v.visa_pk) AS nb_visas_valides
        FROM fact_client_profile f
        JOIN dim_visa v ON f.visa_fk = v.visa_pk
        WHERE v.date_sortie > CURRENT_DATE
          AND v.nb_entree ILIKE '%multiple%'
        GROUP BY f.client_fk;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {row[0]: row[1] for row in rows}

def get_secteur_pk_from_postgres(label):
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT secteur_pk 
        FROM public.dim_secteur 
        WHERE LOWER(label) = %s;
    """, (label.strip().lower(),))

    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        return result[0]
    return None

def get_metier_pk_from_postgres(label):
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT metier_pk 
        FROM public.dim_metier 
        WHERE LOWER(label_jobs) = %s;
    """, (label.strip().lower(),))

    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        return result[0]
    return None

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
        WHERE lower(universite) = %s;
    """, (niveau_etude,))
    
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
        WHERE lower(universite) = %s;
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
        FROM public.dim_contact 
        WHERE firstname = %s AND lastname = %s  AND company = %s  AND typecontact = 'Professionnel';
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
    
def get_project_counts_by_client():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT f.client_fk, COUNT(p.projet_pk) AS nb_projets
        FROM fact_client_profile f
        JOIN dim_projet p ON f.projet_fk = p.projet_pk
        GROUP BY f.client_fk;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {row[0]: row[1] for row in rows}

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
    
def get_nb_experiences_per_client():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            client_fk, 
            COUNT(DISTINCT experience_fk) AS nb_experiences
        FROM 
            fact_client_profile
        WHERE 
            experience_fk IS NOT NULL
        GROUP BY 
            client_fk
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Retourne {client_fk: nb_experiences}
    return {row[0]: row[1] for row in rows}

def get_certification_counts():
    conn = get_postgres_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT client_fk, COUNT(certification_fk) AS nb_certifications
        FROM fact_client_profile
        WHERE certification_fk IS NOT NULL
        GROUP BY client_fk
    """)

    result = cur.fetchall()
    cur.close()
    conn.close()

    return {row[0]: row[1] for row in result}

def get_language_count_per_client():
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT client_fk, COUNT(language_fk) 
        FROM fact_client_profile 
        WHERE language_fk IS NOT NULL 
        GROUP BY client_fk;
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()

    return {row[0]: row[1] for row in results}

def get_secteur_label_from_mongodb(secteur_id):
    collection = get_mongodb_connection().database['secteurs']
    secteur = collection.find_one({"_id": secteur_id})
    if secteur and 'label' in secteur:
        return secteur['label']
    return None


def match_and_display_factcode_client_competence_interest():
    collection, secteur_collection = get_mongodb_connection()
    secteur_label_to_pk = load_dim_secteur()
    metier_label_to_pk = load_dim_metier()
    
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
        "simpleProfile.interests": 1,
        "profile.preferedJobLocations": 1,
        "profile.niveauDetudes": 1,
        "simpleProfile.niveauDetudes": 1,
        "profile.visa": 1,
        "simpleProfile.visa": 1,
        "profile.projets": 1,
        "simpleProfile.projets": 1,
        "profile.proffessionalContacts": 1,
        "simpleProfile.languages": 1,
        "simpleProfile.preferedJobLocations": 1, 
        "profile.secteur": 1, 
        "simpleProfile.secteur": 1,
        "profile.metier": 1,
        "simpleProfile.metier": 1,
        "profile.dureeExperience": 1,
        "simpleProfile.dureeExperience": 1,
        "created_at": 1
    })

    mongo_data_list = list(mongo_data)

    line_count = 0
    counter = 1
    for user in mongo_data_list:
        matricule = user.get("matricule", None)
        client_fk = get_client_fk_from_postgres(matricule)

        certification_counts = get_certification_counts()
        nb_certifications_total = certification_counts.get(client_fk, 0)
        nb_certifications_displayed = False

        language_counts = get_language_count_per_client()
        nb_languages_total = language_counts.get(client_fk, 0)
        nb_languages_displayed = False

        visa_counts = get_visa_counts_by_client()
        nb_visa_valide = visa_counts.get(client_fk, 0)
        visa_displayed = False

        project_counts = get_project_counts_by_client()
        nb_projets_total = project_counts.get(client_fk, 0)
        project_displayed = False

        experience_counts = get_nb_experiences_per_client()
        

        permis_fk_list, competence_fk_list, language_fk_list = [], [], []

        interest_pk_list, job_location_pk_list, study_level_fk_list = [], [], []

        visa_pk_list, project_pk_list, contact_pk_list = [], [], []

        certification_pk_list, experience_fk_list = [], []

        permisConduire = user.get("profile", {}).get("permisConduire", [])
        simpleProfile_permisConduire = user.get("simpleProfile", {}).get("permisConduire", [])

        competenceGenerales = user.get("profile", {}).get("competenceGenerales", [])

        languages = user.get("profile", {}).get("languages", [])
        simpleProfile_languages = user.get("simpleProfile", {}).get("languages", [])

        interests = user.get("profile", {}).get("interests", [])
        Simple_profile_interests = user.get("simpleProfile", {}).get("interests", [])

        preferedJobLocations = user.get("profile", {}).get("preferedJobLocations", [])
        simpleProfile_preferedJobLocations = user.get("simpleProfile", {}).get("preferedJobLocations", [])

        niveau_etudes = user.get("profile", {}).get("niveauDetudes", [])
        niveau_etudes_simple = user.get("simpleProfile", {}).get("niveauDetudes", [])




        visa = user.get("profile", {}).get("visa", [])
        simple_profilevisa = user.get("simpleProfile", {}).get("visa", [])

        projets = user.get("profile", {}).get("projets", [])
        simpleProfile_projets = user.get("simpleProfile", {}).get("projets", [])

        professionalContacts = user.get("profile", {}).get("proffessionalContacts", [])
        simpleProfile_professionalContacts = user.get("simpleProfile", {}).get("proffessionalContacts", [])

        simpleProfile_experiences = user.get("simpleProfile", {}).get("experiences", [])
        experiences = user.get("profile", {}).get("experiences", [])

        certifications = user.get("profile", {}).get("certifications", [])
        simpleProfile_certifications = user.get("simpleProfile", {}).get("certifications", [])

        secteur_profile = user.get("profile", {}).get("secteur")
        secteur_simple = user.get("simpleProfile", {}).get("secteur")

        experience_year = 0
        experience_month = 0

        metier_profile = user.get("profile", {}).get("metier")
        metier_simple = user.get("simpleProfile", {}).get("metier")

        duree_exp_profile = user.get("profile", {}).get("dureeExperience")
        duree_exp_simple = user.get("simpleProfile", {}).get("dureeExperience")

        created_at = user.get("created_at")
        created_at_str = str(created_at) if created_at else None
        created_at_displayed = False
        secteur_pk_list = []
        secteur_pk_list = []
        for secteur_id in [secteur_profile]:
            if secteur_id:
                secteur_obj_id = safe_object_id(str(secteur_id))
                if secteur_obj_id:
                    secteur_doc = secteur_collection.find_one({"_id": secteur_obj_id})
                    label = secteur_doc.get("label", "").lower() if secteur_doc else None
                    pk = secteur_label_to_pk.get(label)
                    if pk:
                        secteur_pk_list.append(pk)

        metier_pk_list = []
        for metier_id in [metier_profile]:
            if metier_id:
                metier_obj_id = safe_object_id(str(metier_id))
                if metier_obj_id:
                    secteur_doc = secteur_collection.find_one({"jobs._id": metier_obj_id})
                    if secteur_doc:
                        for job in secteur_doc.get("jobs", []):
                            if job["_id"] == metier_obj_id:
                                label = job.get("label", "").lower()
                                pk = metier_label_to_pk.get(label)
                                if pk:
                                    metier_pk_list.append(pk)

        # Études depuis profile.niveauDetudes et simpleProfile.niveauDetudes
    
        for niveau in  niveau_etudes_simple:
            niveau_etude_label1 = niveau.get("school", "").strip() if isinstance(niveau, dict) else niveau.strip()
            niveau_etude_label2 = niveau.get("universite", "").strip() if isinstance(niveau, dict) else niveau.strip()
            if niveau_etude_label1:  # Assurez-vous que le label n'est pas vide
                etude_fk = get_etude_pk_from_postgres(niveau_etude_label1)
            elif niveau_etude_label2:  # Assurez-vous que le label n'est pas vide
                etude_fk = get_etude_pk_from_postgres(niveau_etude_label2)
            if etude_fk and str(etude_fk) not in study_level_fk_list:  # Vérification des doublons
                study_level_fk_list.append(str(etude_fk))







        if duree_exp_profile and isinstance(duree_exp_profile, dict) and (duree_exp_profile.get("year") or duree_exp_profile.get("month")):
            experience_year = duree_exp_profile.get("year", 0)
            experience_month = duree_exp_profile.get("month", 0)
        elif duree_exp_simple and isinstance(duree_exp_simple, dict):
            experience_year = duree_exp_simple.get("year", 0)
            experience_month = duree_exp_simple.get("month", 0)
        else:
            experience_year = 0
            experience_month = 0


        secteur_id_list = []

        secteur_profile = user.get("profile", {}).get("secteur")
        secteur_simple = user.get("simpleProfile", {}).get("secteur")

        if secteur_profile:
            secteur_id_list.append(str(secteur_profile))

        if secteur_simple:
            secteur_id_list.append(str(secteur_simple))


        metier_id_list = []
        metier_profile = user.get("profile", {}).get("metier")
        metier_simple = user.get("simpleProfile", {}).get("metier")
        if metier_profile:
            metier_id_list.append(str(metier_profile))

        if metier_simple:
            metier_id_list.append(str(metier_simple))


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

        for interest in interests+Simple_profile_interests:
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


        for projet in projets+simpleProfile_projets:
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

        for visa_item in visa+simple_profilevisa:
            visa_type = visa_item.get("type", "").strip() if isinstance(visa_item, dict) else visa_item.strip()
            visa_pk = get_visa_pk_from_postgres(visa_type)
            if visa_pk:
                visa_pk_list.append(str(visa_pk))

        for contact in professionalContacts+simpleProfile_professionalContacts:
            firstname = contact.get("firstName", "").strip() if isinstance(contact, dict) else ""
            lastname = contact.get("lastName", "").strip() if isinstance(contact, dict) else ""
            company = contact.get("company", "").strip() if isinstance(contact, dict) else ""
            contact_pk = get_contact_pk_from_postgres(firstname, lastname, company)
            if contact_pk:
                contact_pk_list.append(str(contact_pk))

        

        max_length = max(len(permis_fk_list), len(competence_fk_list), len(language_fk_list), 
                         len(interest_pk_list), len(job_location_pk_list), len(study_level_fk_list), 
                         len(visa_pk_list), len(project_pk_list), len(contact_pk_list), 
                         len(certification_pk_list), len(experience_fk_list), len(secteur_id_list), len(metier_id_list), 1)
        experience_year_displayed = False
        created_at_displayed = False

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
            secteur_id = secteur_pk_list[i] if i < len(secteur_pk_list) else None
            metier_id = metier_pk_list[i] if i < len(metier_pk_list) else None

            insert_client_secteur_metier(client_fk, secteur_id, metier_id,counter)



            if not experience_year_displayed:
                year = experience_year
                month = experience_month
                experience_year_displayed = True
            else:
                year = None
                month = None


            if not created_at_displayed:
                created = created_at_str
                created_at_displayed = True
            else:
                created = None


            if not nb_certifications_displayed:
                nb_certif = nb_certifications_total
                nb_certifications_displayed = True
            else:
                nb_certif = 0

            if not nb_languages_displayed:
                nb_langues = nb_languages_total
                nb_languages_displayed = True
            else:
                nb_langues = 0


            if not visa_displayed:
                visa_count_display = nb_visa_valide
                visa_displayed = True
            else:
                visa_count_display = 0


            if not project_displayed:
                project_count_display = nb_projets_total
                project_displayed = True
            else:
                project_count_display = 0

            if not experience_year_displayed:
                nb_exp = experience_counts.get(client_fk, 0)
            else:
                nb_exp = 0

          


         #   print(client_fk, competence_fk, language_fk, interest_pk, job_location_pk,
          #        visa_pk, project_pk, contact_pk, permis_fk, 
           #      certification_pk, experience_fk, secteur_id,metier_id,year,
            #      month,created,nb_certif,nb_langues,visa_count_display,project_count_display,nb_exp,study_level_fk)
            

            line_count += 1
            counter += 1
    
    



    print(f"\nTotal lines: {line_count}")

    

def insert_client_secteur_metier(client_fk, secteur_fk, metier_fk,counter):
    try:
        factcode = generate_factcode(counter)  # Générer le factcode pour chaque ligne

        conn = get_postgres_connection()
        if conn is None:
            return
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO fact_client_profile (
                client_fk, secteur_fk, metier_fk, factcode
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (factcode) DO NOTHING;
        """, (client_fk, secteur_fk, metier_fk, factcode))

        conn.commit()
        print(f"Client {client_fk}, Secteur {secteur_fk}, Métier {metier_fk}, Factcode {factcode} insérés avec succès.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Erreur lors de l'insertion pour client {client_fk}, secteur {secteur_fk}, métier {metier_fk}, factcode {factcode} : {e}")





match_and_display_factcode_client_competence_interest()