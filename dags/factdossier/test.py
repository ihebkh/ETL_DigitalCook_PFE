from pymongo import MongoClient

# Connexion à MongoDB
def get_factures_collection():
    client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
    db = client["PowerBi"]
    return db["factures"]

# Fonction pour extraire tous les noms de service
def extract_all_nom_services():
    factures = get_factures_collection()
    all_services = []

    for facture in factures.find():
        services = facture.get("services", [])
        for service in services:
            nom_service = service.get("nomService")
            if nom_service and nom_service.strip():  # ignorer les vides ou None
                all_services.append(nom_service.strip())

    return list(set(all_services))  # noms uniques

# Générateur de service code formaté
def generate_service_code(counter):
    return f"SERV{counter:04d}"

# Générateur de service_pk (identifiant entier)
def generate_service_pk(counter):
    return counter

# Fonction principale
def generate_services():
    noms = extract_all_nom_services()
    noms.sort()  # facultatif : trier par ordre alphabétique

    print("service_pk | service_code | nom_service")
    print("------------------------------------------")

    for i, nom in enumerate(noms, 1):
        service_pk = generate_service_pk(i)
        service_code = generate_service_code(i)

        print(f"{service_pk:<10} | {service_code:<12} | {nom}")

# Exécution
if __name__ == "__main__":
    generate_services()
