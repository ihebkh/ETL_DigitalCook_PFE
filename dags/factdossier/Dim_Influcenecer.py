from pymongo import MongoClient
import psycopg2

def get_mongodb_collections():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["users"], db["privileges"]

def get_postgres_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def generate_codeinfluencer(index):
    return f"influ{index:04d}"

def extract_users():
    users_collection, privileges_collection = get_mongodb_collections()

    privilege_map = {
        str(p["_id"]): p.get("label", "Non défini")
        for p in privileges_collection.find({}, {"_id": 1, "label": 1})
    }

    users = []
    cursor = users_collection.find({}, {
        "name": 1,
        "last_name": 1,
        "is_admin": 1,
        "privilege": 1
    })

    index = 1
    for user in cursor:
        codeinflu = generate_codeinfluencer(index)
        nom = user.get("name", "")
        prenom = user.get("last_name", "")
        is_admin = user.get("is_admin", False)
        privilege_id = str(user.get("privilege", ""))
        privilege_label = privilege_map.get(privilege_id, "Non défini")
        users.append((codeinflu, nom, prenom, is_admin, privilege_label))
        index += 1

    return users

def insert_users_to_dim_influencer(users):
    conn = get_postgres_connection()
    cur = conn.cursor()

    for user in users:
        cur.execute("""
            INSERT INTO dim_influencer (codeinfluencer, nom, prenom, is_admin, privilege)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (codeinfluencer) DO UPDATE SET
                nom = EXCLUDED.nom,
                prenom = EXCLUDED.prenom,
                is_admin = EXCLUDED.is_admin,
                privilege = EXCLUDED.privilege;
        """, user)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    users = extract_users()
    insert_users_to_dim_influencer(users)
