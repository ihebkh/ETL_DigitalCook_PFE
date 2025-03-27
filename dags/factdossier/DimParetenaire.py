import logging
from pymongo import MongoClient
from bson import ObjectId
import psycopg2

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    """Établit la connexion à MongoDB."""
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    mongo_db = client["PowerBi"]
    collection = mongo_db["universities"]
    return client, mongo_db, collection

def convert_bson(obj):
    """Convertir les ObjectId de BSON en chaîne de caractères."""
    if isinstance(obj, dict):
        return {k: convert_bson(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bson(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    return obj

def generate_partenaire_code(index):
    """Générer un code unique pour chaque partenaire, du format part0001, part0002, etc."""
    return f"part{str(index).zfill(4)}"

def get_postgresql_connection():
    """Établit la connexion à PostgreSQL."""
    conn = psycopg2.connect(
        dbname="DW_DigitalCook", 
        user="postgres", 
        password="admin", 
        host="localhost", 
        port="5432"
    )
    return conn

def insert_partenaire_into_postgres(partenaire_code, partenaire_name, partenaire_type):
    """Insère ou met à jour un partenaire dans la table dim_partenaire."""
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    
    # Requête d'upsert (insertion ou mise à jour)
    insert_query = """
        INSERT INTO public.dim_partenaire (codepartenaire, nom_partenaire, typepartenaire)
        VALUES (%s, %s, %s)
        ON CONFLICT (codepartenaire)
        DO UPDATE SET 
            nom_partenaire = EXCLUDED.nom_partenaire,
            typepartenaire = EXCLUDED.typepartenaire;
    """
    
    cursor.execute(insert_query, (partenaire_code, partenaire_name, partenaire_type))
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Partenaire {partenaire_code} inséré ou mis à jour avec succès.")

def extract_partenaires():
    """Extraire les partenaires professionnels et académiques de MongoDB."""
    client, mongo_db, collection = get_mongodb_connection()
    universities = collection.find({}, {"_id": 0, "partenairesProfessionnel": 1, "partenairesAcademique": 1})

    partenaires_professionnels = []
    partenaires_academiques = []

    # Extraction des partenaires professionnels et académiques
    for university in universities:
        # Extraction des partenaires professionnels
        partenaires_professionnels.extend(university.get("partenairesProfessionnel", []))
        # Extraction des partenaires académiques
        partenaires_academiques.extend(university.get("partenairesAcademique", []))

    # Supprimer les doublons et convertir les ObjectId
    partenaires_professionnels = list(set(convert_bson(partenaires_professionnels)))
    partenaires_academiques = list(set(convert_bson(partenaires_academiques)))

    client.close()

    # Ajouter "professionnel" et "académique" devant chaque partenaire respectivement
    partenaires_professionnels = [f"professionnel {partenaire}" for partenaire in partenaires_professionnels]
    partenaires_academiques = [f"académique {partenaire}" for partenaire in partenaires_academiques]
    
    # Combiner les deux listes de partenaires
    tous_les_partenaires = partenaires_professionnels + partenaires_academiques
    
    # Générer des codes pour chaque partenaire avec un compteur global
    for index, partenaire in enumerate(tous_les_partenaires, start=1):
        code = generate_partenaire_code(index)
        # Insérer ou mettre à jour le partenaire dans PostgreSQL
        insert_partenaire_into_postgres(code, partenaire, "professionnel" if "professionnel" in partenaire else "académique")
    
    logger.info(f"{len(tous_les_partenaires)} partenaires insérés ou mis à jour dans la base de données.")

# Appel de la fonction pour extraire et insérer les partenaires
extract_partenaires()
