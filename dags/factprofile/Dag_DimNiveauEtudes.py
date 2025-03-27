import psycopg2
from pymongo import MongoClient
from datetime import datetime

def get_mongodb_connection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    return client["PowerBi"]["frontusers"]

def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin"
    )

def parse_date(date_input):
    if isinstance(date_input, str):
        try:
            date = datetime.strptime(date_input, "%Y-%m-%d")
            return date.month, date.year
        except:
            try:
                date = datetime.strptime(date_input, "%Y")
                return None, date.year
            except:
                return None, None
    elif isinstance(date_input, dict):
        return date_input.get("month"), date_input.get("year")
    return None, None

def insert_or_update_dim_niveau_etude(cursor, code, label, universite, start_y, start_m, end_y, end_m, diplome, pays, course):
    cursor.execute("""
        INSERT INTO dim_niveau_d_etudes (
            diplome_code, label, universite,
            start_year, start_month, end_year, end_month,
            nom_diplome, pays, course
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (diplome_code)
        DO UPDATE SET
            start_year = EXCLUDED.start_year,
            start_month = EXCLUDED.start_month,
            end_year = EXCLUDED.end_year,
            end_month = EXCLUDED.end_month,
            pays = EXCLUDED.pays,
            course = EXCLUDED.course
    """, (code, label, universite, start_y, start_m, end_y, end_m, diplome, pays, course))

def extract_and_load():
    mongo_collection = get_mongodb_connection()
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()

    compteur = 1

    for doc in mongo_collection.find():
        niveau_etudes = doc.get("simpleProfile", {}).get("niveauDetudes", [])
        for etude in niveau_etudes:
            if not isinstance(etude, dict):
                continue

            code = f"DIP{compteur:03d}"
            compteur += 1

            universite = etude.get("school", "null")
            course = etude.get("course", None)
            label = etude.get("label", "null")
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")

            du = etude.get("duree", {}).get("du", "")
            au = etude.get("duree", {}).get("au", "")
            start_m, start_y = parse_date(du)
            end_m, end_y = parse_date(au)

            start_m = start_m if start_m not in ["", "null"] else None
            start_y = start_y if start_y not in ["", "null"] else None
            end_m = end_m if end_m not in ["", "null"] else None
            end_y = end_y if end_y not in ["", "null"] else None

            insert_or_update_dim_niveau_etude(
                pg_cursor, code, label, universite, start_y, start_m,
                end_y, end_m, diplome, pays, course
            )

        niveau_etudes_v2 = doc.get("profile", {}).get("niveauDetudes", [])
        for etude in niveau_etudes_v2:
            if not isinstance(etude, dict):
                continue

            code = f"DIP{compteur:03d}"
            compteur += 1

            universite = etude.get("universite", "null")
            course = None
            label = etude.get("label", "null")
            pays = etude.get("pays", "N/A")
            diplome = etude.get("nomDiplome", "N/A")

            start_m, start_y = parse_date(etude.get("du", {}))
            end_m, end_y = parse_date(etude.get("au", {}))

            start_m = start_m if start_m not in ["", "null"] else None
            start_y = start_y if start_y not in ["", "null"] else None
            end_m = end_m if end_m not in ["", "null"] else None
            end_y = end_y if end_y not in ["", "null"] else None

            insert_or_update_dim_niveau_etude(
                pg_cursor, code, label, universite, start_y, start_m,
                end_y, end_m, diplome, pays, course
            )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    print(" Données insérées ou mises à jour avec succès.")

if __name__ == "__main__":
    extract_and_load()
