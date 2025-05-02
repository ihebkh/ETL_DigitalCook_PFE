from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime
from collections import defaultdict
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
    
    return collection, secteur_collection

def safe_object_id(id_str):
    try:
        return ObjectId(id_str)
    except InvalidId:
        return None

def generate_fact_pk(counter):
    return f"{counter:04d}"

def load_dim_secteur(cur):
    cur.execute("SELECT secteur_id, nom_secteur FROM public.dim_secteur;")
    return {label.lower(): pk for pk, label in cur if label}

def load_dim_metier(cur):
    cur.execute("SELECT metier_id, nom_metier FROM public.dim_metier;")
    return {label.lower(): pk for pk, label in cur if label}



def get_visa_counts_by_client():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f.client_id, COUNT(v.visa_id) AS nb_visas_valides
                FROM fact_client_profile f
                JOIN dim_visa v ON f.visa_id = v.visa_id
                WHERE v.date_sortie_visa > CURRENT_DATE
                GROUP BY f.client_id;
            """)
            return {row[0]: row[1] for row in cur}


def get_client_fk_from_postgres(cur, matricule):
    matricule_str = str(matricule).strip()
    cur.execute("""
        SELECT client_id
        FROM public.dim_client
        WHERE matricule_client = %s
    """, (matricule_str,))
    result = cur.fetchone()
    return result[0] if result else None

def load_dim_etudes(cur):
    cur.execute("SELECT niveau_etude_id, nom_diplome FROM public.dim_niveau_d_etudes;")
    return {
        nom.strip().lower(): niveau_id
        for niveau_id, nom in cur.fetchall()
        if nom and niveau_id
    }

def get_visa_counts_by_client(cur):
    cur.execute("""
        SELECT f.client_id, COUNT(v.visa_id) AS nb_visas_valides
        FROM fact_client_profile f
        JOIN dim_visa v ON f.visa_id = v.visa_id
        WHERE v.date_sortie_visa > CURRENT_DATE
        GROUP BY f.client_id;
    """)
    return {client_id: count for client_id, count in cur.fetchall()}


def get_client_fk_from_postgres(cur, matricule):
    matricule_str = str(matricule)
    cur.execute("""
        SELECT client_id
        FROM public.dim_client
        WHERE matricule_client = %s
    """, (matricule_str,))
    result = cur.fetchone()
    return result[0] if result else None


def load_dim_etudes(cur):
    cur.execute("SELECT niveau_etude_id, LOWER(nom_diplome) FROM public.dim_niveau_d_etudes;")
    return {label: pk for pk, label in cur.fetchall() if label}



def get_projet_pk_from_postgres(cur, nom_projet):
    cur.execute("""
        SELECT projet_id
        FROM public.dim_projet
        WHERE nom_projet = %s;
    """, (nom_projet,))
    result = cur.fetchone()
    return result[0] if result else None


def load_dim_competences(cur):
    cur.execute("SELECT competence_id, LOWER(nom_competence) FROM public.dim_competence;")
    return {label: pk for pk, label in cur.fetchall() if label}


def load_dim_languages(cur):
    cur.execute("SELECT langue_id, nom_langue, niveau_langue FROM public.dim_langues;")
    return {
        (nom.strip().lower(), niveau.strip().lower()): langue_id
        for langue_id, nom, niveau in cur.fetchall()
        if nom and niveau
    }


def get_interest_pk_from_postgres(cur, interest):
    cur.execute("""
        SELECT interet_id
        FROM public.dim_interests
        WHERE LOWER(nom_interet) = %s;
    """, (interest.strip().lower(),))
    result = cur.fetchone()
    return result[0] if result else None


def get_preferedjoblocations_pk_from_postgres(cur, ville):
    cur.execute("""
        SELECT ville_id
        FROM public.dim_ville
        WHERE nom_ville = %s;
    """, (ville,))
    result = cur.fetchone()
    return result[0] if result else None

def get_permis_fk_from_postgres(cur, permis_code):
    cur.execute("""
        SELECT permis_id
        FROM public.dim_permis_conduire
        WHERE categorie_permis = %s;
    """, (permis_code,))
    result = cur.fetchone()
    return result[0] if result else None


def get_visa_pk_from_postgres(cur, visa_type):
    cur.execute("""
        SELECT visa_id
        FROM public.dim_visa
        WHERE lower(type_visa) = %s;
    """, (visa_type.strip().lower(),))
    result = cur.fetchone()
    return result[0] if result else None


def get_certification_pk_from_postgres(cur, certification_name):
    cur.execute("""
        SELECT certification_id
        FROM public.dim_certification
        WHERE nom_certification = %s;
    """, (certification_name,))
    result = cur.fetchone()
    return result[0] if result else None


def get_project_counts_by_client(cur):
    cur.execute("""
        SELECT client_id, COUNT(projet_id) AS projet_count
        FROM public.fact_client_profile
        GROUP BY client_id;
    """)
    return {row[0]: row[1] for row in cur.fetchall()}


def get_experience_fk_from_postgres(cur, role, entreprise):
    query = "SELECT experience_id FROM public.dim_experience WHERE "
    conditions = []
    values = []

    if role:
        conditions.append("LOWER(role_experience) = %s")
        values.append(role.strip().lower())
    if entreprise:
        conditions.append("LOWER(nom_entreprise) = %s")
        values.append(entreprise.strip().lower())

    if not conditions:
        return None

    query += " AND ".join(conditions)
    cur.execute(query, tuple(values))
    result = cur.fetchone()
    return result[0] if result else None


def get_nb_experiences_per_client(cur):
    cur.execute("""
        SELECT client_id, COUNT(experience_id) AS nb_experiences
        FROM fact_client_profile
        WHERE experience_id IS NOT NULL
        GROUP BY client_id;
    """)
    return {row[0]: row[1] for row in cur.fetchall()}


def get_certification_counts(cur):
    cur.execute("""
        SELECT client_id, COUNT(certification_id) AS nb_certifications
        FROM fact_client_profile
        WHERE certification_id IS NOT NULL
        GROUP BY client_id;
    """)
    return {row[0]: row[1] for row in cur.fetchall()}


def get_language_count_per_client(cur):
    cur.execute("""
        SELECT client_id, COUNT(langue_id) 
        FROM fact_client_profile 
        WHERE langue_id IS NOT NULL 
        GROUP BY client_id;
    """)
    return {row[0]: row[1] for row in cur.fetchall()}



def get_secteur_label_from_mongodb(secteur_id):
    collection = get_mongodb_connection().database['secteurs']
    secteur = collection.find_one({"_id": secteur_id})
    if secteur and 'label' in secteur:
        return secteur['label']
    return None

def get_date_pk_from_postgres(cur, date_obj):
    if not date_obj:
        return None

    cur.execute("""
        SELECT date_id
        FROM dim_Dates
        WHERE code_date = %s;
    """, (date_obj,))
    result = cur.fetchone()
    return result[0] if result else None



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

def extract_date_only(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return None
    return dt.date()

def get_client_pk_by_user_id(cur, user_id):
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
            client_pk = get_client_pk_from_postgres(cur, last_name, first_name)
            if not client_pk:
                client_pk = get_client_pk_from_postgres(cur, first_name, last_name)

        if not client_pk and matricule:
            client_pk = get_client_fk_from_postgres(cur, matricule)

        return client_pk
    return None


def get_client_pk_from_postgres(cur, nom, prenom):
    cur.execute("""
        SELECT client_id 
        FROM public.dim_client
        WHERE LOWER(nom_client) = LOWER(%s) 
          AND LOWER(prenom_client) = LOWER(%s)
        LIMIT 1
    """, (nom, prenom))
    result = cur.fetchone()
    return result[0] if result else None




#matching 

def get_combined_profile_field(user, field_name):
    profile_field = user.get("profile", {}).get(field_name, [])
    simple_profile_field = user.get("simpleProfile", {}).get(field_name, [])
    
    if not isinstance(profile_field, list):
        profile_field = [profile_field] 
    if not isinstance(simple_profile_field, list):
        simple_profile_field = [simple_profile_field]
    
    return profile_field + simple_profile_field

def matchclient():
  with get_postgres_connection() as conn:
   with conn.cursor() as cur:
    collection, secteur_collection = get_mongodb_connection()
    secteur_label_to_pk = load_dim_secteur(cur)
    metier_label_to_pk = load_dim_metier(cur)
    etude_label_to_pk = load_dim_etudes(cur)
    langue_label_level_to_pk = load_dim_languages(cur)
    competence_label_to_pk = load_dim_competences(cur)


    
    mongo_data = collection.find({}, {
        "_id": 0, "matricule": 1,
        "profile.dureeExperience": 1,"simpleProfile.dureeExperience": 1,
        "profile.permisConduire": 1,"simpleProfile.permisConduire": 1,
        "profile.certifications": 1,"simpleProfile.certifications": 1,
        "profile.competenceGenerales": 1,"simpleProfile.competenceGenerales": 1,
        "profile.languages": 1,"simpleProfile.languages": 1,
        "profile.interests": 1, "simpleProfile.interests": 1,
        "profile.preferedJobLocations": 1,"simpleProfile.preferedJobLocations": 1, 
        "profile.niveauDetudes": 1,"simpleProfile.niveauDetudes": 1,
        "profile.visa": 1,"simpleProfile.visa": 1,
        "profile.projets": 1,"simpleProfile.projets": 1,
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

        client_fk = get_client_fk_from_postgres(cur,matricule)
        certification_counts = get_certification_counts(cur)
        language_counts = get_language_count_per_client(cur)
        visa_counts = get_visa_counts_by_client(cur)
        project_counts = get_project_counts_by_client(cur)


        permis_fk_list, competence_fk_list, language_fk_list,interest_pk_list = [],[],[],[]
        interest_pk_list, job_location_pk_list, study_level_fk_list,visa_pk_list = [],[],[],[]
        project_pk_list,certification_pk_list,experience_fk_list = [],[],[]
        secteur_id_list,metier_id_list  = [],[]

        competenceGenerales = get_combined_profile_field(user, "competenceGenerales")
        languages = get_combined_profile_field(user, "languages")
        interests = get_combined_profile_field(user, "interests")
        preferedJobLocations = get_combined_profile_field(user, "preferedJobLocations")
        niveau_etudes = get_combined_profile_field(user, "niveauDetudes")
        visa = get_combined_profile_field(user, "visa")
        projets = get_combined_profile_field(user, "projets")
        experiences = get_combined_profile_field(user, "experiences")
        certifications = get_combined_profile_field(user, "certifications")
        permis = get_combined_profile_field(user, "permisConduire")

        experience_year = None
        experience_month = None

        metier_profile = user.get("profile", {}).get("metier")
        metier_simple = user.get("simpleProfile", {}).get("metier")

        duree_exp_profile = user.get("profile", {}).get("dureeExperience")
        duree_exp_simple = user.get("simpleProfile", {}).get("dureeExperience")


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
        experience_count_displayed =False


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
            if isinstance(niveau, dict):
                niveau_label = niveau.get("label", "").strip().lower()
                etude_fk = etude_label_to_pk.get(niveau_label)
            if etude_fk and str(etude_fk) not in study_level_fk_list:
                study_level_fk_list.append(str(etude_fk))

#duree_experience

        if duree_exp_profile and isinstance(duree_exp_profile, dict) and (duree_exp_profile.get("year") or duree_exp_profile.get("month")):
            experience_year = duree_exp_profile.get("year", None)
            experience_month = duree_exp_profile.get("month", None)
        elif duree_exp_simple and isinstance(duree_exp_simple, dict):
            experience_year = duree_exp_simple.get("year", None)
            experience_month = duree_exp_simple.get("month", None)
        else:
            experience_year = None
            experience_month = None

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
            permis_fk = get_permis_fk_from_postgres(cur,permis)
            if permis_fk:
                permis_fk_list.append(str(permis_fk))

#experience

        for experience in experiences :
            if isinstance(experience, dict):
                role = (experience.get("role", "").strip().lower() or experience.get("poste", "").strip().lower())
                entreprise = experience.get("entreprise", "").strip().lower()
                experience_fk = get_experience_fk_from_postgres(cur,role, entreprise)
                if experience_fk:
                    experience_fk_list.append(str(experience_fk))

#certifications 

        for certification in certifications :
            if isinstance(certification, dict):
                certification_name = certification.get("nomCertification", "").strip()
                certification_pk = get_certification_pk_from_postgres(cur,certification_name)
                if certification_pk:
                    certification_pk_list.append(str(certification_pk))
            elif isinstance(certification, str):
                certification_name = certification.strip()
                certification_pk = get_certification_pk_from_postgres(cur,certification_name)
                if certification_pk:
                    certification_pk_list.append(str(certification_pk))

#competences gernales 
        for competence in competenceGenerales:
            label = competence.strip().lower()
            competence_fk = competence_label_to_pk.get(label)
            if competence_fk:
                competence_fk_list.append(str(competence_fk))

# langue 

        for language in languages:
            label = language.get("label", "").strip().lower() if isinstance(language, dict) else language.strip().lower()
            level = language.get("level", "").strip().lower() if isinstance(language, dict) else ""
            langue_fk = langue_label_level_to_pk.get((label, level))
            if langue_fk:
                language_fk_list.append(str(langue_fk))

#interests 

        for interest in interests:
            interest_pk = get_interest_pk_from_postgres(cur,interest)
            if interest_pk:
                interest_pk_list.append(str(interest_pk))


#locations

        for location in preferedJobLocations:
            ville = location.get("ville", "").strip()
            preferedjoblocations_pk = get_preferedjoblocations_pk_from_postgres(cur,ville)
            if preferedjoblocations_pk:
                job_location_pk_list.append(str(preferedjoblocations_pk))
#projets 

        for projet in projets:
            if isinstance(projet, dict):
                nom_projet = projet.get("nomProjet", "").strip()
                projet_pk = get_projet_pk_from_postgres(cur,nom_projet)
                if projet_pk:
                    project_pk_list.append(str(projet_pk))

#visa 

        for visa_item in visa:
            visa_type = visa_item.get("type", "").strip() if isinstance(visa_item, dict) else visa_item.strip()
            visa_pk = get_visa_pk_from_postgres(cur,visa_type)
            if visa_pk:
                visa_pk_list.append(str(visa_pk))
                

        

        max_length = max(len(permis_fk_list), len(competence_fk_list), len(language_fk_list), 
                         len(interest_pk_list), len(job_location_pk_list), len(study_level_fk_list), 
                         len(visa_pk_list), len(project_pk_list), 
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
            certification_pk = certification_pk_list[i] if i < len(certification_pk_list) else None
            experience_fk = experience_fk_list[i] if i < len(experience_fk_list) else None
            secteur_id = secteur_pk_list[i] if i < len(secteur_pk_list) else None
            metier_id = metier_pk_list[i] if i < len(metier_pk_list) else None

            nb_certif = certification_counts.get(client_fk)
            nb_certifications_displayed = False

            nb_languages_total = language_counts.get(client_fk, None)
            nb_languages_displayed = False

            nb_visa_valide = visa_counts.get(client_fk, None)
            visa_displayed = False

            nb_projets_total = project_counts.get(client_fk, None)
            project_displayed = False


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
                dim_date_pk = get_date_pk_from_postgres(cur,created_date)
                created_at_displayed = True
            else:
                dim_date_pk = None

# certifications :  

            if not nb_certifications_displayed:
                nb_certif = nb_certif
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
            if not experience_count_displayed:
                nb_exp = nb_exp = get_nb_experiences_per_client(cur).get(client_fk, None)
                experience_count_displayed = True
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



            load_fact_date(client_fk, secteur_id, metier_id,counter,competence_fk,language_fk,
                          interest_pk,certification_pk,visa_pk,job_location_pk,
                           project_pk,permis_fk,experience_fk,year,month,nb_certif,nb_langues,visa_count_display
                           ,project_count_display,nb_exp,study_level_fk,dispo,age,dim_date_pk)
            

            line_count += 1
            counter += 1
    
    print(f"\nTotal lines: {line_count}")

def load_fact_date(client_fk, secteur_fk, metier_fk, counter, competence_fk, language_fk,
                    interest_fk, certification_pk, visa_pk, job_location_pk,
                    project_pk, permis_fk, experience_fk, year, month,
                    nb_certif, nb_langues, visa_count_display, project_count_display, nb_exp,
                    study_level_fk, dispo, age, dim_date_pk):
    try:
        fact_pk = generate_fact_pk(counter)
        conn = get_postgres_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO fact_client_profile (
                client_id, secteur_id, metier_id, fact_id,
                competence_generale_id, langue_id, interet_id,
                certification_id, visa_id,
                location_preferee_emploi_id, projet_id, permis_id,
                experience_id, annee_experience, mois_experience,
                nombre_certifications, nombre_langues, nombre_visas_valides,
                nombre_projets, nombre_experiences, etude_id, disponibilite, age_client,
                date_id
            )
            VALUES (%s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s)
            ON CONFLICT (fact_id) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                secteur_id = EXCLUDED.secteur_id,
                metier_id = EXCLUDED.metier_id,
                competence_generale_id = EXCLUDED.competence_generale_id,
                langue_id = EXCLUDED.langue_id,
                interet_id = EXCLUDED.interet_id,
                certification_id = EXCLUDED.certification_id,
                visa_id = EXCLUDED.visa_id,
                location_preferee_emploi_id = EXCLUDED.location_preferee_emploi_id,
                projet_id = EXCLUDED.projet_id,
                permis_id = EXCLUDED.permis_id,
                experience_id = EXCLUDED.experience_id,
                annee_experience = EXCLUDED.annee_experience,
                mois_experience = EXCLUDED.mois_experience,
                nombre_certifications = EXCLUDED.nombre_certifications,
                nombre_langues = EXCLUDED.nombre_langues,
                nombre_visas_valides = EXCLUDED.nombre_visas_valides,
                nombre_projets = EXCLUDED.nombre_projets,
                nombre_experiences = EXCLUDED.nombre_experiences,
                etude_id = EXCLUDED.etude_id,
                disponibilite = EXCLUDED.disponibilite,
                age_client = EXCLUDED.age_client,
                date_id = EXCLUDED.date_id;
        """, (
            client_fk, secteur_fk, metier_fk, fact_pk,
            competence_fk, language_fk, interest_fk,
            certification_pk, visa_pk,
            job_location_pk, project_pk, permis_fk,
            experience_fk, year, month,
            nb_certif, nb_langues, visa_count_display,
            project_count_display, nb_exp,
            study_level_fk, dispo, age, dim_date_pk
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(fact_pk)

    except Exception as e:
        print(f" Erreur pour client {client_fk}, fact_pk iteration {fact_pk} : {e}")



dag = DAG(
    'dag_client_profile',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

"""
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
wait_dim_dates_task = ExternalTaskSensor(
    task_id='wait_for_dim_dates',
    external_dag_id='dim_dates_dag',
    external_task_id='load_dim_dates', 
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag 
)
"""
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

#[wait_dim_metier,wait_dim_secteur,wait_dim_certifications,
#wait_dim_clients,wait_dim_competences,wait_dim_experience,
#wait_dim_interests,wait_dim_languages,wait_dim_niveau_etudes,
#wait_dim_permis,wait_dim_locations,wait_projects,wait_visa,wait_dim_dates_task]>> 
task_run_etl>>end_task