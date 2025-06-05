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
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    return hook.get_conn()



def get_mongodb_connection():
    MONGO_URI = Variable.get("MONGO_URI")
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
    cur.execute("SELECT niveau_etude_id, LOWER(nom_diplome) FROM public.dim_niveau_d_etudes;")
    return {label: pk for pk, label in cur.fetchall() if label}

def get_projet_pk_from_postgres(cur, nom_projet):
    if not nom_projet:
        return None
        
    cur.execute("""
        SELECT projet_id, code_projet 
        FROM public.dim_projet
        WHERE LOWER(nom_projet) = LOWER(%s);
    """, (nom_projet.strip(),))
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
        FROM public.dim_interet
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

def get_certification_pk_from_postgres(cur, certification_data):
    if not certification_data:
        return None
        
    if isinstance(certification_data, str):
        nom_certification = certification_data.strip()
    elif isinstance(certification_data, dict):
        nom_certification = certification_data.get("nomCertification", "").strip()
    else:
        return None
        
    if not nom_certification:
        return None
        
    cur.execute("""
        SELECT certification_id
        FROM public.dim_certification
        WHERE LOWER(nom_certification) = LOWER(%s)
    """, (nom_certification,))
    result = cur.fetchone()
    return result[0] if result else None

def get_experience_fk_from_postgres(cur, role):
    query = "SELECT experience_id FROM public.dim_experience WHERE "
    conditions = []
    values = []

    if role:
        conditions.append("LOWER(role_experience) = %s")
        values.append(role.strip().lower())

    if not conditions:
        return None

    query += " AND ".join(conditions)
    cur.execute(query, tuple(values))
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
                "profile.dureeExperience": 1, "simpleProfile.dureeExperience": 1,
                "profile.certifications": 1, "simpleProfile.certifications": 1,
                "profile.competenceGenerales": 1, "simpleProfile.competenceGenerales": 1,
                "profile.languages": 1, "simpleProfile.languages": 1,
                "profile.interests": 1, "simpleProfile.interests": 1,
                "profile.preferedJobLocations": 1, "simpleProfile.preferedJobLocations": 1,
                "profile.niveauDetudes": 1, "simpleProfile.niveauDetudes": 1,
                "profile.projets": 1, "simpleProfile.projets": 1,
                "profile.secteur": 1, "simpleProfile.secteur": 1,
                "profile.metier": 1, "simpleProfile.metier": 1,
                "created_at": 1,
                "profile.disponibilite": 1, "simpleProfile.disponibilite": 1,
                "profile.birthDate": 1, "simpleProfile.birthDate": 1,
                "profile.experiences": 1, "simpleProfile.experiences": 1,
            })

            mongo_data_list = list(mongo_data)
            line_count = 0
            counter = 1

            for user in mongo_data_list:
                matricule = user.get("matricule", None)
                client_fk = get_client_fk_from_postgres(cur, matricule)

                competence_fk_list = []
                language_fk_list = []
                interest_pk_list = []
                job_location_pk_list = []
                study_level_fk_list = []
                project_pk_list = []
                certification_pk_list = []
                experience_fk_list = []
                secteur_id_list = []
                metier_id_list = []

                competenceGenerales = get_combined_profile_field(user, "competenceGenerales")
                languages = get_combined_profile_field(user, "languages")
                interests = get_combined_profile_field(user, "interests")
                preferedJobLocations = get_combined_profile_field(user, "preferedJobLocations")
                niveau_etudes = get_combined_profile_field(user, "niveauDetudes")
                projets = get_combined_profile_field(user, "projets")
                experiences = get_combined_profile_field(user, "experiences")
                certifications = get_combined_profile_field(user, "certifications")

                experience_year = None
                experience_month = None

                metier_profile = user.get("profile", {}).get("metier")
                metier_simple = user.get("simpleProfile", {}).get("metier")

                duree_exp_profile = user.get("profile", {}).get("dureeExperience")
                duree_exp_simple = user.get("simpleProfile", {}).get("dureeExperience")

                created_at = user.get("created_at")
                disponibilite = user.get("profile", {}).get("disponibilite") or user.get("simpleProfile", {}).get("disponibilite")
                birth_date = user.get("profile", {}).get("birthDate") or user.get("simpleProfile", {}).get("birthDate")

                secteur_profile = user.get("profile", {}).get("secteur")
                secteur_simple = user.get("simpleProfile", {}).get("secteur")

                # Process secteur
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

                # Process metier
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

                # Process other fields
                for niveau in niveau_etudes:
                    if isinstance(niveau, dict):
                        niveau_label = niveau.get("label", "").strip().lower()
                        etude_fk = etude_label_to_pk.get(niveau_label)
                        if etude_fk and str(etude_fk) not in study_level_fk_list:
                            study_level_fk_list.append(str(etude_fk))

                if duree_exp_profile and isinstance(duree_exp_profile, dict):
                    experience_year = duree_exp_profile.get("year", None)
                    experience_month = duree_exp_profile.get("month", None)
                elif duree_exp_simple and isinstance(duree_exp_simple, dict):
                    experience_year = duree_exp_simple.get("year", None)
                    experience_month = duree_exp_simple.get("month", None)

                # Process experiences
                for experience in experiences:
                    if isinstance(experience, dict):
                        role = (experience.get("role", "").strip().lower() or experience.get("poste", "").strip().lower())
                        experience_fk = get_experience_fk_from_postgres(cur, role)
                        if experience_fk:
                            experience_fk_list.append(str(experience_fk))

                # Process other fields
                for competence in competenceGenerales:
                    label = competence.strip().lower()
                    competence_fk = competence_label_to_pk.get(label)
                    if competence_fk:
                        competence_fk_list.append(str(competence_fk))

                for language in languages:
                    label = language.get("label", "").strip().lower() if isinstance(language, dict) else language.strip().lower()
                    level = language.get("level", "").strip().lower() if isinstance(language, dict) else ""
                    langue_fk = langue_label_level_to_pk.get((label, level))
                    if langue_fk:
                        language_fk_list.append(str(langue_fk))

                for interest in interests:
                    interest_pk = get_interest_pk_from_postgres(cur, interest)
                    if interest_pk:
                        interest_pk_list.append(str(interest_pk))

                for location in preferedJobLocations:
                    ville = location.get("ville", "").strip()
                    preferedjoblocations_pk = get_preferedjoblocations_pk_from_postgres(cur, ville)
                    if preferedjoblocations_pk:
                        job_location_pk_list.append(str(preferedjoblocations_pk))

                # Process projets
                for projet in projets:
                    if isinstance(projet, dict):
                        nom_projet = projet.get("nomProjet", "").strip()
                        if nom_projet:
                            projet_pk = get_projet_pk_from_postgres(cur, nom_projet)
                            if projet_pk:
                                project_pk_list.append(str(projet_pk))
                            else:
                                logger.warning(f"Projet non trouvé dans dim_projet: {nom_projet}")

                # Process certifications
                for certification in certifications:
                    certification_pk = get_certification_pk_from_postgres(cur, certification)
                    if certification_pk:
                        certification_pk_list.append(str(certification_pk))
                    else:
                        cert_name = certification.get("nomCertification", "") if isinstance(certification, dict) else certification
                        logger.warning(f"Certification non trouvée dans dim_certification: {cert_name}")

                max_length = max(len(competence_fk_list), len(language_fk_list),
                               len(interest_pk_list), len(job_location_pk_list),
                               len(study_level_fk_list),
                               len(project_pk_list), len(certification_pk_list),
                               len(experience_fk_list), len(secteur_id_list),
                               len(metier_id_list), 1)

                for i in range(max_length):
                    competence_fk = competence_fk_list[i] if i < len(competence_fk_list) else None
                    language_fk = language_fk_list[i] if i < len(language_fk_list) else None
                    interest_pk = interest_pk_list[i] if i < len(interest_pk_list) else None
                    job_location_pk = job_location_pk_list[i] if i < len(job_location_pk_list) else None
                    study_level_fk = study_level_fk_list[i] if i < len(study_level_fk_list) else None
                    project_pk = project_pk_list[i] if i < len(project_pk_list) else None
                    certification_pk = certification_pk_list[i] if i < len(certification_pk_list) else None
                    experience_fk = experience_fk_list[i] if i < len(experience_fk_list) else None
                    secteur_id = secteur_pk_list[i] if i < len(secteur_pk_list) else None
                    metier_id = metier_pk_list[i] if i < len(metier_pk_list) else None

                    # Calculate values for first iteration only
                    if i == 0:
                        created_date = extract_date_only(created_at)
                        dim_date_pk = get_date_pk_from_postgres(cur, created_date)
                        age = calculate_age(birth_date)
                    else:
                        dim_date_pk = None
                        age = None

                    # Insert into fact table
                    load_fact_date(
                        client_fk, secteur_id, metier_id, counter,
                        competence_fk, language_fk, interest_pk,
                        certification_pk, job_location_pk,
                        project_pk, experience_fk, experience_year,
                        experience_month, study_level_fk,
                        disponibilite, age, dim_date_pk
                    )

                    line_count += 1
                    counter += 1

            print(f"\nTotal lines: {line_count}")

def load_fact_date(client_fk, secteur_fk, metier_fk, counter, competence_fk,
                  language_fk, interest_fk, certification_pk,
                  job_location_pk, project_pk, experience_fk, year, month,
                  study_level_fk, dispo, age, dim_date_pk):
    try:
        fact_pk = generate_fact_pk(counter)
        conn = get_postgres_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO fact_client_profile (
                client_id, secteur_id, metier_id, fact_id,
                competence_generale_id, langue_id, interet_id,
                certification_id, location_preferee_emploi_id,
                projet_id, experience_id, annee_experience,
                mois_experience, etude_id,
                disponibilite, age_client, date_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fact_id) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                secteur_id = EXCLUDED.secteur_id,
                metier_id = EXCLUDED.metier_id,
                competence_generale_id = EXCLUDED.competence_generale_id,
                langue_id = EXCLUDED.langue_id,
                interet_id = EXCLUDED.interet_id,
                certification_id = EXCLUDED.certification_id,
                location_preferee_emploi_id = EXCLUDED.location_preferee_emploi_id,
                projet_id = EXCLUDED.projet_id,
                experience_id = EXCLUDED.experience_id,
                annee_experience = EXCLUDED.annee_experience,
                mois_experience = EXCLUDED.mois_experience,
                etude_id = EXCLUDED.etude_id,
                disponibilite = EXCLUDED.disponibilite,
                age_client = EXCLUDED.age_client,
                date_id = EXCLUDED.date_id;
        """, (
            client_fk, secteur_fk, metier_fk, fact_pk,
            competence_fk, language_fk, interest_fk,
            certification_pk, job_location_pk,
            project_pk, experience_fk, year, month,
            study_level_fk, dispo, age, dim_date_pk
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(fact_pk)

    except Exception as e:
        print(f"Erreur pour client {client_fk}, fact_pk iteration {fact_pk} : {e}")

dag = DAG(
    'dag_client_profile',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

task_run_etl = PythonOperator(
    task_id='run_etl_fact_client_profile',
    python_callable=matchclient,
    provide_context=True,
    dag=dag
) 

start_task = PythonOperator(
    task_id='start_task',
    python_callable=lambda: logger.info("Starting region extraction process..."),
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=lambda: logger.info("Region extraction process completed."),
    dag=dag
)

start_task>>task_run_etl>>end_task