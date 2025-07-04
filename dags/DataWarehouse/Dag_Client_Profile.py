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

def get_client_fk_from_postgres(cur, matricule):
    matricule_str = str(matricule).strip()
    cur.execute("""
        SELECT client_id
        FROM public.dim_client
        WHERE matricule_client = %s
    """, (matricule_str,))
    result = cur.fetchone()
    return result[0] if result else None

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
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                collection, secteur_collection = get_mongodb_connection()
                secteur_label_to_pk = load_dim_secteur(cur)
                langue_label_level_to_pk = load_dim_languages(cur)
                competence_label_to_pk = load_dim_competences(cur)

                try:
                    mongo_count = collection.count_documents({})
                    print(f"[DEBUG] Nombre total de documents dans MongoDB frontusers: {mongo_count}")
                except Exception as e:
                    print(f"[ERREUR] Impossible de compter les documents MongoDB: {e}")
                    raise

                mongo_data = collection.find({}, {
                    "_id": 0, "matricule": 1,
                    "profile.dureeExperience": 1, "simpleProfile.dureeExperience": 1,
                    "profile.certifications": 1, "simpleProfile.certifications": 1,
                    "profile.competenceGenerales": 1, "simpleProfile.competenceGenerales": 1,
                    "profile.languages": 1, "simpleProfile.languages": 1,
                    "profile.interests": 1, "simpleProfile.interests": 1,
                    "profile.projets": 1, "simpleProfile.projets": 1,
                    "profile.secteur": 1, "simpleProfile.secteur": 1,
                    "created_at": 1,
                    "profile.birthDate": 1, "simpleProfile.birthDate": 1,
                    "profile.experiences": 1, "simpleProfile.experiences": 1,
                })

                mongo_data_list = list(mongo_data)
                print(f"[DEBUG] Nombre de documents récupérés par find: {len(mongo_data_list)}")
                if not mongo_data_list:
                    print("[ERREUR] Aucun document trouvé dans la collection MongoDB frontusers. Vérifiez la connexion et la présence de données.")
                    return

                line_count = 0
                counter = 1

                for user in mongo_data_list:
                    matricule = user.get("matricule", None)
                    client_fk = get_client_fk_from_postgres(cur, matricule)

                    competence_fk_list = []
                    language_fk_list = []
                    interest_pk_list = []
                    project_pk_list = []
                    certification_pk_list = []
                    experience_fk_list = []
                    age_list = []
                    experience_year_list = []
                    experience_month_list = []

                    competenceGenerales = get_combined_profile_field(user, "competenceGenerales")
                    languages = get_combined_profile_field(user, "languages")
                    interests = get_combined_profile_field(user, "interests")
                    projets = get_combined_profile_field(user, "projets")
                    experiences = get_combined_profile_field(user, "experiences")
                    certifications = get_combined_profile_field(user, "certifications")

                    duree_exp_profile = user.get("profile", {}).get("dureeExperience")
                    duree_exp_simple = user.get("simpleProfile", {}).get("dureeExperience")

                    created_at = user.get("created_at")
                    birth_date = user.get("profile", {}).get("birthDate") or user.get("simpleProfile", {}).get("birthDate")

                    secteur_profile = user.get("profile", {}).get("secteur")
                    secteur_simple = user.get("simpleProfile", {}).get("secteur")

                    # Process secteur - get single secteur_id
                    secteur_id = None
                    for secteur_id_value in [secteur_profile]:
                        if secteur_id_value:
                            secteur_obj_id = safe_object_id(str(secteur_id_value))
                            if secteur_obj_id:
                                secteur_doc = secteur_collection.find_one({"_id": secteur_obj_id})
                                label = secteur_doc.get("label", "").lower() if secteur_doc else None
                                pk = secteur_label_to_pk.get(label)
                                if pk:
                                    secteur_id = pk
                                    break

                    # Process date - get single value
                    created_date = extract_date_only(created_at)
                    dim_date_pk = get_date_pk_from_postgres(cur, created_date)

                    # Process age - add to list like other fields
                    age = calculate_age(birth_date)
                    if age is not None:
                        age_list.append(age)

                    # Process experience duration - add to lists like other fields
                    if duree_exp_profile and isinstance(duree_exp_profile, dict):
                        experience_year = duree_exp_profile.get("year", None)
                        experience_month = duree_exp_profile.get("month", None)
                        if experience_year is not None:
                            experience_year_list.append(experience_year)
                        if experience_month is not None:
                            experience_month_list.append(experience_month)
                    elif duree_exp_simple and isinstance(duree_exp_simple, dict):
                        experience_year = duree_exp_simple.get("year", None)
                        experience_month = duree_exp_simple.get("month", None)
                        if experience_year is not None:
                            experience_year_list.append(experience_year)
                        if experience_month is not None:
                            experience_month_list.append(experience_month)

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
                                   len(interest_pk_list),
                                   len(project_pk_list), len(certification_pk_list),
                                   len(experience_fk_list), len(age_list), 
                                   len(experience_year_list), len(experience_month_list), 1)

                    for i in range(max_length):
                        competence_fk = competence_fk_list[i] if i < len(competence_fk_list) else None
                        language_fk = language_fk_list[i] if i < len(language_fk_list) else None
                        interest_pk = interest_pk_list[i] if i < len(interest_pk_list) else None
                        project_pk = project_pk_list[i] if i < len(project_pk_list) else None
                        certification_pk = certification_pk_list[i] if i < len(certification_pk_list) else None
                        experience_fk = experience_fk_list[i] if i < len(experience_fk_list) else None
                        age = age_list[i] if i < len(age_list) else None
                        experience_year = experience_year_list[i] if i < len(experience_year_list) else None
                        experience_month = experience_month_list[i] if i < len(experience_month_list) else None

                        # Insert into fact table - secteur_id and dim_date_pk are repeated for each iteration
                        load_fact_date(
                            client_fk, secteur_id, counter,
                            competence_fk, language_fk, interest_pk,
                            certification_pk,
                            project_pk, experience_fk, experience_year,
                            experience_month, age, dim_date_pk
                        )

                        line_count += 1
                        counter += 1

                print(f"\nTotal lines: {line_count}")
    except Exception as e:
        print(f"[ERREUR] Exception dans matchclient: {e}")
        import traceback
        traceback.print_exc()

def load_fact_date(client_fk, secteur_fk, counter, competence_fk,
                  language_fk, interest_fk, certification_pk
                  , project_pk, experience_fk, year, month, age, dim_date_pk):
    try:
        fact_pk = generate_fact_pk(counter)
        conn = get_postgres_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO dim_client_profile (
                client_id, secteur_id, client_profile_id,
                competence_id, langue_id, interet_id,
                certification_id,
                projet_id, experience_id, annee_experience,
                mois_experience, age_client, date_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
            ON CONFLICT (client_profile_id) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                secteur_id = EXCLUDED.secteur_id,
                competence_id = EXCLUDED.competence_id,
                langue_id = EXCLUDED.langue_id,
                interet_id = EXCLUDED.interet_id,
                certification_id = EXCLUDED.certification_id,
                projet_id = EXCLUDED.projet_id,
                experience_id = EXCLUDED.experience_id,
                annee_experience = EXCLUDED.annee_experience,
                mois_experience = EXCLUDED.mois_experience,
                age_client = EXCLUDED.age_client,
                date_id = EXCLUDED.date_id;
        """, (
            client_fk, secteur_fk, fact_pk,
            competence_fk, language_fk, interest_fk,
            certification_pk,
            project_pk, experience_fk, year, month, age, dim_date_pk
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
    catchup=False,
    schedule_interval=None
)

task_run_etl = PythonOperator(
    task_id='dim_client_profile',
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