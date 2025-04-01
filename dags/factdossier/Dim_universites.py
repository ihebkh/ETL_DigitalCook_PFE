import logging
from pymongo import MongoClient
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgresql_connection():
    hook = PostgresHook(postgres_conn_id="postgres")
    return hook.get_conn()

def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["universities"]
    return client, collection

def sanitize_for_json(doc):
    if isinstance(doc, dict):
        return {k: sanitize_for_json(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [sanitize_for_json(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc

def load_villes():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT ville_pk, name FROM public.dim_ville;")
    villes = {nom.strip().lower(): pk for pk, nom in cur.fetchall() if nom}
    cur.close()
    conn.close()
    return villes

def load_filieres():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT filiere_pk, nomfiliere, domaine, diplome, prerequis, codepostal FROM public.dim_filiere;")
    filieres = [dict(zip(
        ["filiere_pk", "nomfiliere", "domaine", "diplome", "prerequis", "codepostal"],
        [r[0], *(v.strip().lower() if v else None for v in r[1:])])) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return filieres

def load_contacts():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT contact_pk, firstname, lastname, poste, adresse, company, typecontact FROM public.dim_contact;")
    contacts = [dict(zip(
        ["contact_pk", "firstname", "lastname", "poste", "adresse", "company", "typecontact"],
        [r[0], *(v.strip().lower() if v else None for v in r[1:])])) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return contacts

def load_partenaires():
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute("SELECT partenaire_pk, nom_partenaire, typepartenaire FROM public.dim_partenaire;")
    partenaires = [dict(zip(
        ["partenaire_pk", "nom_partenaire", "typepartenaire"],
        [r[0], *(v.strip().lower() if v else None for v in r[1:])])) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return partenaires

def generate_codeuniv(counter):
    return f"univ{counter:04d}"

def upsert_fact_universite(universite_pk, codeuniversite, nom_uni, pays, date_creation, ville_pk, filiere_pk, contact_pk, part_acad_pk, part_pro_pk):
    conn = get_postgresql_connection()
    cur = conn.cursor()
    cur.execute(""" 
        INSERT INTO dim_universite (universite_pk, codeuniversite, nom, pays, date_creation, ville_fk, filiere_fk, contact_fk, partenaire_academique_fk, partenaire_pro_fk)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (universite_pk) DO UPDATE SET
            nom = EXCLUDED.nom,
            pays = EXCLUDED.pays,
            date_creation = EXCLUDED.date_creation,
            ville_fk = EXCLUDED.ville_fk,
            filiere_fk = EXCLUDED.filiere_fk,
            contact_fk = EXCLUDED.contact_fk,
            partenaire_academique_fk = EXCLUDED.partenaire_academique_fk,
            partenaire_pro_fk = EXCLUDED.partenaire_pro_fk;
    """, (universite_pk, codeuniversite, nom_uni, pays, date_creation, ville_pk, filiere_pk, contact_pk, part_acad_pk, part_pro_pk))
    conn.commit()
    cur.close()
    conn.close()

def extract_universites(**kwargs):
    client, collection = get_mongodb_connection()
    raw_data = list(collection.find({}, {
        "nom": 1, "pays": 1, "created_at": 1, "ville": 1,
        "filiere": 1, "contact": 1, "partenairesAcademique": 1,
        "partenairesProfessionnel": 1
    }))
    client.close()
    cleaned_data = [sanitize_for_json(doc) for doc in raw_data]
    kwargs['ti'].xcom_push(key='universites', value=cleaned_data)

def insert_universites(**kwargs):
    universites = kwargs['ti'].xcom_pull(task_ids='extract_universites_task', key='universites')
    if not universites:
        logger.info("Aucune donnée à insérer.")
        return

    villes_dict = load_villes()
    filieres_pg = load_filieres()
    contacts_pg = load_contacts()
    partenaires_pg = load_partenaires()

    universite_code_map = {}
    code_counter = 1
    total = 0
    universite_pk_counter = 1

    for doc in universites:
        nom_uni = doc.get("nom", "Université inconnue")
        nom_uni_cleaned = nom_uni.strip().lower()

        if nom_uni_cleaned not in universite_code_map:
            universite_code_map[nom_uni_cleaned] = generate_codeuniv(code_counter)
            code_counter += 1

        codeuniversite = universite_code_map[nom_uni_cleaned]
        pays = doc.get("pays")
        date_creation = doc.get("created_at")
        if date_creation:
            try:
                date_creation = datetime.fromisoformat(date_creation)
            except:
                date_creation = None

        villes = [v.strip().lower() for v in doc.get("ville", []) if isinstance(v, str)] if isinstance(doc.get("ville"), list) else []
        ville_pk_list = [villes_dict.get(v) for v in villes]

        filiere_pk_list, contact_pk_list, part_acad_list, part_pro_list = [], [], [], []

        for f in doc.get("filiere", []):
            if isinstance(f, dict):
                for f_pg in filieres_pg:
                    match = True
                    for mongo_key, pg_key in [
                        ("nomfiliere", "nomfiliere"),
                        ("domaine", "domaine"),
                        ("diplome", "diplome"),
                        ("prerequis", "prerequis"),
                        ("codePostal", "codepostal")
                    ]:
                        val_mongo = f.get(mongo_key)
                        if val_mongo and f_pg.get(pg_key) != val_mongo.strip().lower():
                            match = False
                            break
                    if match:
                        filiere_pk_list.append(f_pg["filiere_pk"])
                        break

        for c in doc.get("contact", []):
            if isinstance(c, dict):
                nom = (c.get("nom") or "").strip().lower()
                poste = (c.get("poste") or "").strip().lower()
                adresse = (c.get("adresse") or "").strip().lower()
                for c_pg in contacts_pg:
                    if (
                        (c_pg["firstname"] and nom in c_pg["firstname"]) or
                        (c_pg["lastname"] and nom in c_pg["lastname"]) or
                        (c_pg["poste"] and poste in c_pg["poste"]) or
                        (c_pg["adresse"] and adresse in c_pg["adresse"]) or
                        (c_pg["company"] and nom in c_pg["company"]) or
                        (c_pg["typecontact"] == "université")
                    ):
                        contact_pk_list.append(c_pg["contact_pk"])
                        break

        for p in doc.get("partenairesAcademique", []):
            nom = p.strip().lower()
            for p_pg in partenaires_pg:
                if p_pg["nom_partenaire"] == nom and p_pg["typepartenaire"] == "académique":
                    part_acad_list.append(p_pg["partenaire_pk"])
                    break

        for p in doc.get("partenairesProfessionnel", []):
            nom = p.strip().lower()
            for p_pg in partenaires_pg:
                if p_pg["nom_partenaire"] == nom and p_pg["typepartenaire"] == "professionnel":
                    part_pro_list.append(p_pg["partenaire_pk"])
                    break

        max_len = max(len(ville_pk_list), len(filiere_pk_list), len(contact_pk_list), len(part_acad_list), len(part_pro_list), 1)

        for i in range(max_len):
            ville_pk = ville_pk_list[i] if i < len(ville_pk_list) else None
            filiere_pk = filiere_pk_list[i] if i < len(filiere_pk_list) else None
            contact_pk = contact_pk_list[i] if i < len(contact_pk_list) else None
            acad_pk = part_acad_list[i] if i < len(part_acad_list) else None
            pro_pk = part_pro_list[i] if i < len(part_pro_list) else None

            universite_pk = universite_pk_counter
            print(f"{universite_pk} | {codeuniversite} → {nom_uni} ({pays}) | ville_fk={ville_pk}, filiere_fk={filiere_pk}, contact_fk={contact_pk}, acad_fk={acad_pk}, pro_fk={pro_pk}")
            upsert_fact_universite(universite_pk, codeuniversite, nom_uni, pays, date_creation, ville_pk, filiere_pk, contact_pk, acad_pk, pro_pk)
            universite_pk_counter += 1
            total += 1

    logger.info(f"Total universités insérées ou mises à jour : {total}")

# DAG

dag = DAG(
    dag_id='dag_dimuniversites',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/2 * * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_universites_task',
    python_callable=extract_universites,
    provide_context=True,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_universites_task',
    python_callable=insert_universites,
    provide_context=True,
    dag=dag
)

wait_dim_ville_destination = ExternalTaskSensor(
    task_id='wait_for_dim_ville_destination',
    external_dag_id='dag_dim_villes_destinations',
    external_task_id='load_villes_and_destinations_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_partenaire = ExternalTaskSensor(
    task_id='wait_for_dim_partenaire',
    external_dag_id='dag_dim_partenaire',
    external_task_id='load_partenaires_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_filiere = ExternalTaskSensor(
    task_id='wait_for_dim_filiere',
    external_dag_id='dag_dim_filiere',
    external_task_id='load_filieres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_contact = ExternalTaskSensor(
    task_id='wait_for_dim_contact',
    external_dag_id='dag_dim_contact',
    external_task_id='load_contacts_postgres',
    mode='poke',
    timeout=600,
    poke_interval=30,
    dag=dag
)

wait_dim_contact >> wait_dim_filiere >> wait_dim_partenaire >> wait_dim_ville_destination >> extract_task >> insert_task
