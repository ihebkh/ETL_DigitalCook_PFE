from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# Liste des variantes des pays
countries = ["abouda bazoula", "abouda miboun","abouda", "tunisie", "algeria", "algerie", "france", "algerian"]

# Fonction pour regrouper les variantes en un seul pays
def group_countries(countries):
    grouped = {}
    
    for country in countries:
        # Comparer chaque pays avec les pays déjà regroupés
        found = False
        for key in grouped.keys():
            if fuzz.ratio(country, key) > 50:  # seuil de similarité
                grouped[key].append(country)
                found = True
                break
        
        if not found:
            grouped[country] = [country]
    
    return grouped

# Exécution
grouped_countries = group_countries(countries)

# Affichage des résultats
for group, countries_in_group in grouped_countries.items():
    print(f"Groupe: {group} -> {countries_in_group}")
