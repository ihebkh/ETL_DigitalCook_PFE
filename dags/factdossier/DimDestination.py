from pymongo import MongoClient
import psycopg2
from datetime import datetime

def get_postgresql_connection():
    return psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
MONGO_DB = "PowerBi"
MONGO_COLLECTION = "dossiers"

def get_mongodb_connection():
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return collection

def extract_destinations():
    collection = get_mongodb_connection()
    results = collection.find({}, {"firstStep.destination": 1, "_id": 0})
    destinations = []

    for record in results:
        if "firstStep" in record and "destination" in record["firstStep"]:
            for destination in record["firstStep"]["destination"]:
                destinations.append(destination)

    return destinations


def generate_destination_codes(destinations):
    destinations_with_codes = []
    dest_counter = 1

    for destination in destinations:
        destination_code = f"dest{str(dest_counter).zfill(4)}"
        destinations_with_codes.append({
            "destination_name": destination,
            "destination_code": destination_code
        })
        dest_counter += 1

    return destinations_with_codes

def transform_data(destinations):
    unique_destinations = list(set(destinations))
    
    return generate_destination_codes(unique_destinations)

destinations = extract_destinations()

transformed_destinations = transform_data(destinations)

for entry in transformed_destinations:
    print(f"Destination: {entry['destination_name']}, Code: {entry['destination_code']}")


def load_into_postgres(destinations_with_codes):
    conn = get_postgresql_connection()
    cur = conn.cursor()
    insert_update_query = """
    INSERT INTO dim_destination (destination_name, destination_code)
    VALUES (%s, %s)
    ON CONFLICT (destination_name) DO UPDATE 
    SET destination_name = EXCLUDED.destination_name
    """

    for record in destinations_with_codes:
        values = (record["destination_name"], record["destination_code"])
        cur.execute(insert_update_query, values)
    
    conn.commit()
    cur.close()
    conn.close()

    print(f"{len(destinations_with_codes)} records inserted/updated into PostgreSQL.")
load_into_postgres(transformed_destinations)
