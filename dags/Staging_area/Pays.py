import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from pymongo import MongoClient, UpdateOne

url = "https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")
table = soup.find("table", {"class": "wikitable"})

countries = []

for row in table.find_all("tr")[1:]:
    cols = row.find_all("td")
    if len(cols) >= 2:
        raw_country = cols[0].text.strip()
        raw_code = cols[1].text.strip()

      
        clean_country = re.sub(r'\[.*?\]', '', raw_country)
        clean_country = re.sub(r'\(.*?\)', '', clean_country).strip()

   
        clean_code = re.sub(r'\[.*?\]', '', raw_code)
        clean_code = re.sub(r'\(.*?\)', '', clean_code).strip().upper()

        countries.append({"country": clean_country, "alpha2_code": clean_code})

excel_path = r"C:\Users\khmir\Desktop\ETL_DigitalCook_PFE\data\country_codes.xlsx"
os.makedirs(os.path.dirname(excel_path), exist_ok=True)

df = pd.DataFrame(countries)
df.to_excel(excel_path, index=False)

print(f"✅ Fichier Excel sauvegardé dans : {excel_path}")

client = MongoClient("mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/")
db = client["PowerBi"]
collection = db["pays"]

operations = [
    UpdateOne(
        {"alpha2_code": doc["alpha2_code"]},
        {"$set": doc},
        upsert=True
    ) for doc in countries
]

if operations:
    result = collection.bulk_write(operations)
    print(f" MongoDB : {result.upserted_count} insérés, {result.modified_count} mis à jour.")
else:
    print(" Aucune opération MongoDB effectuée.")
