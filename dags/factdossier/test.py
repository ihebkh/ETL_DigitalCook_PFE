import re
from pymongo import MongoClient

# Fonction pour obtenir la connexion MongoDB
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "universities"

    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return client, mongo_db, collection

# Fonction pour extraire les langues depuis MongoDB
def extract_languages_from_mongodb():
    client, mongo_db, collection = get_mongodb_connection()

    # Récupération des documents avec uniquement le champ 'langue'
    universities = collection.find({}, {"_id": 0, "langue": 1})

    languages = []
    for university in universities:
        # Vérifier si le champ 'langue' existe dans le document
        if "langue" in university:
            languages.extend(university["langue"])

    client.close()
    return languages

# Fonction pour diviser la langue en label et level
def split_language(language):
    # Utiliser une expression régulière pour séparer le label et le niveau avec une gestion d'espaces flexible
    match = re.match(r"([A-Za-z\s]+)\s*\(\s*([^)]+)\s*\)\s*:\s*(.*)", language)
    if match:
        label = match.group(1).strip()  # Label est tout avant la parenthèse
        level = f"({match.group(2)}) {match.group(3).strip()}"  # Level est tout après la parenthèse et sans les ':'
        return label, level
    else:
        # Si aucun format valide n'est trouvé (par exemple, pas de parenthèses ni de ':')
        label = language.strip()  # Afficher uniquement le label sans niveau
        level = "Niveau manquant"  # Ajouter une mention pour signaler l'absence de niveau
        return label, level

# Exemple d'appel de la fonction et affichage des langues extraites ligne par ligne
languages = extract_languages_from_mongodb()
print("Langues extraites de MongoDB :")
for language in languages:
    label, level = split_language(language)
    print(f"Label: {label}, Level: {level}")
