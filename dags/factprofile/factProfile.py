from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime
from collections import defaultdict
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.empty import EmptyOperator 


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    
    collection = mongo_db[MONGO_COLLECTION]
    secteur_collection = mongo_db["secteurdactivities"]
    parrainage_collection = mongo_db["parrainages"]
    
    return collection, secteur_collection,parrainage_collection

def safe_object_id(id_str):
    try:
        return ObjectId(id_str)
    except InvalidId:
        return None

def generate_fact_pk(counter):
    return f"{counter:04d}"

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
        FROM public.dim_competence 
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
        WHERE categorie = %s;
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

    query = "SELECT experience_pk FROM public.dim_experience WHERE "
    conditions = []
    values = []

    if role:
        conditions.append("role = %s")
        values.append(role.strip())
    if entreprise:
        conditions.append("entreprise = %s")
        values.append(entreprise.strip())
    if type_contrat:
        conditions.append("typecontrat = %s")
        values.append(type_contrat.strip())

    if not conditions:
        return None

    query += " AND ".join(conditions)

    cur.execute(query, tuple(values))
    result = cur.fetchone()

    cur.close()
    conn.close()

    return result[0] if result else None
    
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

def get_date_pk_from_postgres(date_obj):
    if not date_obj:
        return None

    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT date_pk
        FROM dim_Dates
        WHERE datecode = %s;
    """, (date_obj,))
    
    result = cur.fetchone()
    cur.close()
    conn.close()

    return result[0] if result else None

# calcule d'age 

def calculate_age(birth_date):
    if not birth_date:
        return None

    if isinstance(birth_date, str):
        try:
            birth_date = datetime.fromisoformat(birth_date)
        except ValueError:
            return None

    today = datetime.today()
    age = today.year - birth_date.year - (
        (today.month, today.day) < (birth_date.month, birth_date.day)
    )
    return age

#extraction de date

def extract_date_only(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return None
    return dt.date()

def get_client_pk_by_user_id(user_id):
    collection, _, _ = get_mongodb_connection()

    matched_user = collection.find_one({"_id": user_id})
    if not matched_user:
        matched_user = collection.find_one({"userId": user_id})

    if matched_user:
        full_name = matched_user.get("fullName", "").strip()
        matricule = str(matched_user.get("matricule", "")).strip()
        nom_prenom = full_name.split()

        client_pk = None
        if len(nom_prenom) >= 2:
            first_name = nom_prenom[1]
            last_name = nom_prenom[0]
            client_pk = get_client_pk_from_postgres(last_name, first_name)
            if not client_pk:
                client_pk = get_client_pk_from_postgres(first_name, last_name)

        if not client_pk and matricule:
            client_pk = get_client_fk_from_postgres(matricule)

        return client_pk
    return None

def get_client_pk_from_postgres(nom, prenom):
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT client_pk FROM public.dim_client
        WHERE LOWER(nom) = LOWER(%s) AND LOWER(prenom) = LOWER(%s)
        LIMIT 1
    """, (nom, prenom))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None

def get_parrainage_data():
    _, _, parrainages_col = get_mongodb_connection()

    parrainage_counts = defaultdict(int)
    filleuls_par_client = defaultdict(list)

    for doc in parrainages_col.find():
        parrain_pk = doc.get("client_pk")
        user_id = doc.get("userId")

        if parrain_pk and user_id:
            filleul_pk = get_client_pk_by_user_id(user_id)
            if filleul_pk and filleul_pk != parrain_pk:
                parrainage_counts[parrain_pk] += 1
                filleuls_par_client[parrain_pk].append(filleul_pk)
            else:
                filleuls_par_client[parrain_pk].append(None)

    return parrainage_counts, filleuls_par_client

#matching 

def get_combined_profile_field(user, field_name):
    return user.get("profile", {}).get(field_name, []) + user.get("simpleProfile", {}).get(field_name, [])

def matchclient():
    collection, secteur_collection, _ = get_mongodb_connection()
    secteur_label_to_pk = load_dim_secteur()
    metier_label_to_pk = load_dim_metier()
    parrainage_counts, filleuls_by_client = get_parrainage_data()
    nb_parrainages_displayed = False
    
    mongo_data = collection.find({}, {
        "_id": 0, "matricule": 1,
        "profile.dureeExperience": 1,"simpleProfile.dureeExperience": 1,
        "profile.permisConduire": 1,"simpleProfile.permisConduire": 1,
        "profile.certifications": 1,"simpleProfile.certifications": 1,
        "profile.competenceGenerales": 1,"simpleProfile.competenceGenerales": 1,
        "profile.languages": 1,"simpleProfile.languages": 1,
        "profile.interests": 1,"simpleProfile.interests": 1,
        "profile.preferedJobLocations": 1,"simpleProfile.preferedJobLocations": 1, 
        "profile.niveauDetudes": 1,
        "simpleProfile.niveauDetudes": 1,
        "profile.visa": 1,"simpleProfile.visa": 1,
        "profile.projets": 1,"simpleProfile.projets": 1,
        "profile.proffessionalContacts": 1,"simpleProfile.proffessionalContacts": 1,
        "profile.secteur": 1, "simpleProfile.secteur": 1,
        "profile.metier": 1,"simpleProfile.metier": 1,
        "created_at": 1,
        "profile.disponibilite": 1,"simpleProfile.disponibilite": 1,
        "profile.birthDate": 1,"simpleProfile.birthDate": 1,
        "profile.experiences": 1,"simpleProfile.experiences": 1,
        
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
        

        permis_fk_list, competence_fk_list, language_fk_list,interest_pk_list = [],[],[],[]
        interest_pk_list, job_location_pk_list, study_level_fk_list,visa_pk_list = [],[],[],[]
        project_pk_list, contact_pk_list,certification_pk_list,experience_fk_list = [],[],[],[]
        secteur_id_list,metier_id_list  = [],[]

        competenceGenerales = get_combined_profile_field(user, "competenceGenerales")
        languages = get_combined_profile_field(user, "languages")
        interests = get_combined_profile_field(user, "interests")
        preferedJobLocations = get_combined_profile_field(user, "preferedJobLocations")
        niveau_etudes = get_combined_profile_field(user, "niveauDetudes")
        visa = get_combined_profile_field(user, "visa")
        projets = get_combined_profile_field(user, "projets")
        professionalContacts = get_combined_profile_field(user, "proffessionalContacts")
        experiences = get_combined_profile_field(user, "experiences")
        certifications = get_combined_profile_field(user, "certifications")
        permis = get_combined_profile_field(user, "permisConduire")

        experience_year = 0
        experience_month = 0

        metier_profile = user.get("profile", {}).get("metier")
        metier_simple = user.get("simpleProfile", {}).get("metier")

        duree_exp_profile = user.get("profile", {}).get("dureeExperience")
        duree_exp_simple = user.get("simpleProfile", {}).get("dureeExperience")

        metier_profile = user.get("profile", {}).get("metier")
        metier_simple = user.get("simpleProfile", {}).get("metier")

        created_at = user.get("created_at")
        created_at_displayed = False


        disponibilite = user.get("profile", {}).get("disponibilite") or user.get("simpleProfile", {}).get("disponibilite")
        disponibilite_displayed = False

        birth_date = user.get("profile", {}).get("birthDate") or user.get("simpleProfile", {}).get("birthDate")
        birth_date_displayed = False


        secteur_profile = user.get("profile", {}).get("secteur")
        secteur_simple = user.get("simpleProfile", {}).get("secteur")

        experience_year_displayed = False
        created_at_displayed = False


#secteur

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

#metier

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

#niveau d etude
    
        for niveau in niveau_etudes:
            niveau_etude_label1 = niveau.get("school", "").strip() if isinstance(niveau, dict) else niveau.strip()
            niveau_etude_label2 = niveau.get("universite", "").strip() if isinstance(niveau, dict) else niveau.strip()
            if niveau_etude_label1:
                etude_fk = get_etude_pk_from_postgres(niveau_etude_label1)
            elif niveau_etude_label2:
                etude_fk = get_etude_pk_from_postgres(niveau_etude_label2)
            if etude_fk and str(etude_fk) not in study_level_fk_list:
                study_level_fk_list.append(str(etude_fk))

#duree_experience

        if duree_exp_profile and isinstance(duree_exp_profile, dict) and (duree_exp_profile.get("year") or duree_exp_profile.get("month")):
            experience_year = duree_exp_profile.get("year", 0)
            experience_month = duree_exp_profile.get("month", 0)
        elif duree_exp_simple and isinstance(duree_exp_simple, dict):
            experience_year = duree_exp_simple.get("year", 0)
            experience_month = duree_exp_simple.get("month", 0)
        else:
            experience_year = 0
            experience_month = 0


 

        if secteur_profile:
            secteur_id_list.append(str(secteur_profile))

        if secteur_simple:
            secteur_id_list.append(str(secteur_simple))



        if metier_profile:
            metier_id_list.append(str(metier_profile))

        if metier_simple:
            metier_id_list.append(str(metier_simple))
#permis de conduire

        for permis in  permis:
            permis_fk = get_permis_fk_from_postgres(permis)
            if permis_fk:
                permis_fk_list.append(str(permis_fk))

#experience

        for experience in experiences :
            if isinstance(experience, dict):
                role = experience.get("role", "").strip()
                entreprise = experience.get("entreprise", "").strip()
                type_contrat = experience.get("typeContrat", {}).get("value", "").strip()
                experience_fk = get_experience_fk_from_postgres(role, entreprise, type_contrat)
                if experience_fk:
                    experience_fk_list.append(str(experience_fk))

#certifications 

        for certification in certifications :
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

#competences gernales 
        for competence in competenceGenerales:
            competence_fk = get_competence_fk_from_postgres(competence)
            if competence_fk:
                competence_fk_list.append(str(competence_fk))
# langue 

        for language in languages :
            language_label = language.get("label", "").strip() if isinstance(language, dict) else language.strip()
            language_level = language.get("level", "").strip() if isinstance(language, dict) else ""
            language_fk = get_language_fk_from_postgres(language_label, language_level)
            if language_fk:
                language_fk_list.append(str(language_fk))
#interests 

        for interest in interests:
            interest_pk = get_interest_pk_from_postgres(interest)
            if interest_pk:
                interest_pk_list.append(str(interest_pk))
#locations

        for location in preferedJobLocations:
            pays = location.get("pays", "").strip()
            ville = location.get("ville", "").strip()
            region = location.get("region", "").strip()
            preferedjoblocations_pk = get_preferedjoblocations_pk_from_postgres(pays, ville, region)
            if preferedjoblocations_pk:
                job_location_pk_list.append(str(preferedjoblocations_pk))
#projets 

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

#visa 

        for visa_item in visa:
            visa_type = visa_item.get("type", "").strip() if isinstance(visa_item, dict) else visa_item.strip()
            visa_pk = get_visa_pk_from_postgres(visa_type)
            if visa_pk:
                visa_pk_list.append(str(visa_pk))
#contact pro 

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
                         len(certification_pk_list), len(experience_fk_list), len(secteur_id_list), len(metier_id_list), 1)
        

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

# experience : year 

            if not experience_year_displayed:
                year = experience_year
                month = experience_month
                experience_year_displayed = True
            else:
                year = None
                month = None
# creation : 

            if not created_at_displayed:
                created_date = extract_date_only(created_at)
                dim_date_pk = get_date_pk_from_postgres(created_date)
                created_at_displayed = True
            else:
                dim_date_pk = None

# certifications :  

            if not nb_certifications_displayed:
                nb_certif = nb_certifications_total
                nb_certifications_displayed = True
            else:
                nb_certif = None
# langue : 
            if not nb_languages_displayed:
                nb_langues = nb_languages_total
                nb_languages_displayed = True
            else:
                nb_langues = None

# visa :
            if not visa_displayed:
                visa_count_display = nb_visa_valide
                visa_displayed = True
            else:
                visa_count_display = None
# projet :

            if not project_displayed:
                project_count_display = nb_projets_total
                project_displayed = True
            else:
                project_count_display = None
# experience :
            if not experience_year_displayed:
                nb_exp = experience_counts.get(client_fk, 0)
            else:
                nb_exp = None
# disponibilite :
            if not disponibilite_displayed:
                dispo = disponibilite
                disponibilite_displayed = True
            else:
                dispo = None

# age :
            if not birth_date_displayed:
                age = calculate_age(birth_date)
                birth_date_displayed = True
            else:
                age = None


            if not nb_parrainages_displayed:
                nb_parrainages = parrainage_counts.get(client_fk, 0)
                nb_parrainages_displayed = True
            else:
                nb_parrainages = None

            load_fact_date(client_fk, secteur_id, metier_id,counter,competence_fk,language_fk,
                          interest_pk,certification_pk,contact_pk,visa_pk,job_location_pk,
                           project_pk,permis_fk,experience_fk,year,month,nb_certif,nb_langues,visa_count_display
                           ,project_count_display,nb_exp,study_level_fk,dispo,age,dim_date_pk,nb_parrainages)
            

            line_count += 1
            counter += 1
    
    print(f"\nTotal lines: {line_count}")

def load_fact_date(client_fk, secteur_fk, metier_fk, counter, competence_fk, language_fk,
                    interest_fk, certification_pk, contact_pk, visa_pk, job_location_pk,
                    project_pk, permis_fk, experience_fk, year, month,
                    nb_certif, nb_langues, visa_count_display, project_count_display, nb_exp,
                    study_level_fk, dispo, age, dim_date_pk, nb_parrainages):
    try:
        fact_pk = generate_fact_pk(counter)
        conn = get_postgres_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO fact_client_profile (
                client_fk, secteur_fk, metier_fk, fact_pk,
                competencegenerales_fk, language_fk, interests_fk,
                certification_fk, contact_fk, visa_fk,
                preferedjoblocations_fk, projet_fk, permis_fk,
                experience_fk, experience_year, experience_month,
                nbr_certif, nbr_langue, nbr_visa_valide,
                nbr_projet, nb_experience, etude_fk, disponibilite, age,
                date_fk, nb_parrainages
            )
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s)
            ON CONFLICT (fact_pk) DO UPDATE SET
                client_fk = EXCLUDED.client_fk,
                secteur_fk = EXCLUDED.secteur_fk,
                metier_fk = EXCLUDED.metier_fk,
                competencegenerales_fk = EXCLUDED.competencegenerales_fk,
                language_fk = EXCLUDED.language_fk,
                interests_fk = EXCLUDED.interests_fk,
                certification_fk = EXCLUDED.certification_fk,
                contact_fk = EXCLUDED.contact_fk,
                visa_fk = EXCLUDED.visa_fk,
                preferedjoblocations_fk = EXCLUDED.preferedjoblocations_fk,
                projet_fk = EXCLUDED.projet_fk,
                permis_fk = EXCLUDED.permis_fk,
                experience_fk = EXCLUDED.experience_fk,
                experience_year = EXCLUDED.experience_year,
                experience_month = EXCLUDED.experience_month,
                nbr_certif = EXCLUDED.nbr_certif,
                nbr_langue = EXCLUDED.nbr_langue,
                nbr_visa_valide = EXCLUDED.nbr_visa_valide,
                nbr_projet = EXCLUDED.nbr_projet,
                nb_experience = EXCLUDED.nb_experience,
                etude_fk = EXCLUDED.etude_fk,
                disponibilite = EXCLUDED.disponibilite,
                age = EXCLUDED.age,
                date_fk = EXCLUDED.date_fk,
                nb_parrainages = EXCLUDED.nb_parrainages;
        """, (
            client_fk, secteur_fk, metier_fk, fact_pk,
            competence_fk, language_fk, interest_fk,
            certification_pk, contact_pk, visa_pk,
            job_location_pk, project_pk, permis_fk,
            experience_fk, year, month,
            nb_certif, nb_langues, visa_count_display,
            project_count_display, nb_exp,
            study_level_fk, dispo, age, dim_date_pk, nb_parrainages
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(fact_pk)

    except Exception as e:
        print(f" Erreur pour client {client_fk}, fact_pk iteration {fact_pk} : {e}")



dag = DAG(
    'dag_fact',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

wait_dim_secteur = ExternalTaskSensor(
    task_id='wait_for_dim_secteur',
    external_dag_id='dag_dim_secteur',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_clients = ExternalTaskSensor(
    task_id='wait_for_dim_clients',
    external_dag_id='Dag_DimClients',             
    external_task_id='load_data',                 
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)



wait_dim_metier = ExternalTaskSensor(
    task_id='wait_for_dim_metier',
    external_dag_id='Dag_Metier',
    external_task_id='load_jobs_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)
wait_dim_certifications = ExternalTaskSensor(
    task_id='wait_for_dim_certifications',
    external_dag_id='Dag_Dimcertifications',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_competences = ExternalTaskSensor(
    task_id='wait_for_dim_competences',
    external_dag_id='Dag_DimCompetences',  
    external_task_id='load_competences',  
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_experience = ExternalTaskSensor(
    task_id='wait_for_dim_experience',
    external_dag_id='dag_dim_experience',     
    external_task_id='end_task',            
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_interests = ExternalTaskSensor(
    task_id='wait_for_dim_interests',
    external_dag_id='Dag_DimInterests',
    external_task_id='load_interests',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_languages = ExternalTaskSensor(
    task_id='wait_for_dim_languages',
    external_dag_id='Dag_DimLanguages',
    external_task_id='load_languages',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)
wait_dim_niveau_etudes = ExternalTaskSensor(
    task_id='wait_for_dim_niveau_etudes',
    external_dag_id='dag_dim_niveau_etudes',
    external_task_id='load_dim_niveau_etudes',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_permis = ExternalTaskSensor(
    task_id='wait_for_dim_permis',
    external_dag_id='Dag_DimPermisConduire',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)
wait_dim_locations = ExternalTaskSensor(
    task_id='wait_for_dim_preferedjoblocations',
    external_dag_id='Dag_DimpreferedJobLocations',
    external_task_id='load_into_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)
wait_projects = ExternalTaskSensor(
        task_id='wait_dim_projet',
        external_dag_id='Dag_DimProjet',
        external_task_id='load_into_postgres',
        mode='poke',
        timeout=600,
        poke_interval=30
)

wait_visa = ExternalTaskSensor(
        task_id='wait_dim_visa',
        external_dag_id='visa_dag',
        external_task_id='load_into_postgres',
        mode='poke',
        timeout=600,
        poke_interval=30
)


task_run_etl = PythonOperator(
    task_id='run_etl_fact_client_profile',
    python_callable=matchclient,
    provide_context=True,
    dag=dag
)
end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)

[wait_dim_metier,wait_dim_secteur,wait_dim_certifications,
wait_dim_clients,wait_dim_competences,wait_dim_experience,
wait_dim_interests,wait_dim_languages,wait_dim_niveau_etudes,
wait_dim_permis,wait_dim_locations,wait_projects,wait_visa]>> task_run_etl>>end_task