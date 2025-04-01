from pymongo import MongoClient
import pprint

def get_offres_collection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    client = MongoClient(MONGO_URI)
    db = client["PowerBi"]
    return db["offredemplois"]

def extract_offres_emploi():
    offres_collection = get_offres_collection()

    cursor = offres_collection.find({
        "isDeleted": False
    }, {
        "titre": 1,
        "secteur": 1,
        "metier": 1,
        "typeContrat": 1,
        "tempsDeTravail": 1,
        "societe": 1,
        "lieuSociete": 1,
        "minSalaire": 1,
        "maxSalaire": 1,
        "deviseSalaire": 1,
        "salaireBrutPar": 1,
        "niveauDexperience": 1,
        "langue": 1,
        "disponibilite": 1,
        "pays": 1,
        "onSiteOrRemote": 1,
        "entreprise": 1,
        "ville": 1
    })

    for i, doc in enumerate(cursor, start=1):
        offre = {
            "titre": doc.get("titre", "—"),
            "secteur_id": str(doc.get("secteur", "—")),
            "metier_id": str(doc.get("metier", "—")),
            "typeContrat": doc.get("typeContrat", "—"),
            "tempsDeTravail": doc.get("tempsDeTravail", "—"),
            "societe": doc.get("societe", "—"),
            "lieuSociete": doc.get("lieuSociete", "—"),
            "minSalaire": doc.get("minSalaire", 0),
            "maxSalaire": doc.get("maxSalaire", 0),
            "deviseSalaire": doc.get("deviseSalaire", ""),
            "salaireBrutPar": doc.get("salaireBrutPar", ""),
            "niveauDexperience": doc.get("niveauDexperience", "—"),
            "langues": doc.get("langue", []),
            "disponibilite": doc.get("disponibilite", "—"),
            "pays": doc.get("pays", "—"),
            "onSiteOrRemote": doc.get("onSiteOrRemote", "—"),
            "entreprise_id": str(doc.get("entreprise", "—")),
            "ville": doc.get("ville", "—")
        }

        print(f"\n📄 Offre {i}:")
        pprint.pprint(offre)

if __name__ == "__main__":
    extract_offres_emploi()
