from pymongo import MongoClient
from bson import ObjectId
import psycopg2

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def get_mongo_collections():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return client, db["offredemplois"], db["entreprises"], db["secteurdactivities"]

def load_dim_secteur(cur):
    cur.execute("SELECT secteur_pk, LOWER(label) FROM public.dim_secteur;")
    return {label: pk for pk, label in cur.fetchall() if label}

def load_dim_metier(cur):
    cur.execute("SELECT metier_pk, LOWER(label_jobs) FROM public.dim_metier;")
    return {label: pk for pk, label in cur.fetchall() if label}

def get_entreprise_pk_by_nom(cur, nom):
    cur.execute("SELECT entreprise_pk FROM public.dim_entreprise WHERE LOWER(nom) = LOWER(%s);", (nom,))
    row = cur.fetchone()
    return row[0] if row else None

def load_offre_to_postgres(values, cur):
    cur.execute("""
        INSERT INTO public.offreemploi (
            offre_pk, offre_code, titre, secteur_fk, metier_fk, entreprise_fk,
            type_contrat, temps_de_travail, societe, lieu_societe,
            devise_salaire, salaire_brut_par, niveau_experience, disponibilite,
            pays, on_site_or_remote
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (offre_pk) DO UPDATE SET
            titre = EXCLUDED.titre,
            secteur_fk = EXCLUDED.secteur_fk,
            metier_fk = EXCLUDED.metier_fk,
            entreprise_fk = EXCLUDED.entreprise_fk,
            type_contrat = EXCLUDED.type_contrat,
            temps_de_travail = EXCLUDED.temps_de_travail,
            societe = EXCLUDED.societe,
            lieu_societe = EXCLUDED.lieu_societe,
            devise_salaire = EXCLUDED.devise_salaire,
            salaire_brut_par = EXCLUDED.salaire_brut_par,
            niveau_experience = EXCLUDED.niveau_experience,
            disponibilite = EXCLUDED.disponibilite,
            pays = EXCLUDED.pays,
            on_site_or_remote = EXCLUDED.on_site_or_remote;
    """, values)

def extract_and_insert_offres():
    client, offres_col, entreprises_col, secteurs_col = get_mongo_collections()
    conn = get_postgres_connection()
    cur = conn.cursor()

    secteur_map = load_dim_secteur(cur)
    metier_map = load_dim_metier(cur)

    cursor = offres_col.find({"isDeleted": False})
    seen_titles = set()
    counter = 1

    for doc in cursor:
        titre = doc.get("titre", "").strip()
        if not titre or titre.lower() in seen_titles:
            continue
        seen_titles.add(titre.lower())

        offre_code = f"OFFR{str(counter).zfill(4)}"
        entreprise_fk = secteur_fk = metier_fk = None

        # Entreprise
        ent_id = doc.get("entreprise")
        if ent_id and ObjectId.is_valid(ent_id):
            ent_doc = entreprises_col.find_one({"_id": ObjectId(ent_id)})
            if ent_doc and ent_doc.get("nom"):
                entreprise_fk = get_entreprise_pk_by_nom(cur, ent_doc["nom"])

        # Secteur et métier
        secteur_id = doc.get("secteur")
        metier_ids = doc.get("metier", [])
        if not isinstance(metier_ids, list):
            metier_ids = [metier_ids]

        if secteur_id and ObjectId.is_valid(secteur_id):
            secteur_doc = secteurs_col.find_one({"_id": ObjectId(secteur_id)})
            if secteur_doc:
                secteur_label = secteur_doc.get("label", "").strip().lower()
                secteur_fk = secteur_map.get(secteur_label)

                if "jobs" in secteur_doc:
                    for job in secteur_doc["jobs"]:
                        if str(job["_id"]) in metier_ids:
                            metier_label = job.get("label", "").strip().lower()
                            metier_fk = metier_map.get(metier_label)
                            break

        # Données pour insertion
        values = (
            counter,
            offre_code,
            titre,
            secteur_fk,
            metier_fk,
            entreprise_fk,
            doc.get("typeContrat", "—"),
            doc.get("tempsDeTravail", "—"),
            doc.get("societe", "—"),
            doc.get("lieuSociete", "—"),
            doc.get("deviseSalaire", "—"),
            doc.get("salaireBrutPar", "—"),
            doc.get("niveauDexperience", "—"),
            doc.get("disponibilite", "—"),
            doc.get("pays", "—"),
            doc.get("onSiteOrRemote", "—")
        )

        # Affichage simple
        print(f"📄 Offre {counter} | Code: {offre_code} | Titre: {titre}")

        # Insertion
        load_offre_to_postgres(values, cur)
        counter += 1

    conn.commit()
    cur.close()
    conn.close()
    client.close()

if __name__ == "__main__":
    extract_and_insert_offres()
