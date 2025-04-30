import json
import logging
from pymongo import MongoClient
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongodb_connection():
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        
        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        logger.info("MongoDB connection successful.")
        return client, mongo_db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def extract_from_mongodb(**kwargs):
    try:
        client, mongo_db = get_mongodb_connection()
        
        # Extraction des données depuis la collection 'frontusers'
        frontusers_collection = mongo_db["frontusers"]
        frontusers_data = list(frontusers_collection.find({}, {"_id": 0, "profile.preferedJobLocations.ville": 1, "simpleProfile.preferedJobLocations.ville": 1}))
        
        # Extraction des données depuis la collection 'dossier'
        dossier_collection = mongo_db["dossier"]
        dossier_data = list(dossier_collection.find({}, {"_id": 0, "preferedJobLocations.ville": 1}))
        
        # Fusionner les résultats
        combined_data = frontusers_data + dossier_data
        
        # Serialize les résultats
        for i in range(len(combined_data)):
            combined_data[i] = json.loads(json.dumps(combined_data[i], default=str))  # Handle ObjectId serialization
        
        client.close()
        logger.info(f"Extracted {len(combined_data)} records from MongoDB (frontusers + dossier).")
        
        return combined_data
    except Exception as e:
        logger.error(f"Error extracting data from MongoDB: {e}")
        raise

def transform_data(mongo_data):
    try:
        job_locations = []

        for user in mongo_data:
            for profile_key in ["profile", "simpleProfile"]:
                if isinstance(user, dict) and profile_key in user and isinstance(user[profile_key], dict):
                    profile = user[profile_key]
                    if "preferedJobLocations" in profile:
                        locations = profile["preferedJobLocations"]

                        if isinstance(locations, list):
                            for loc in locations:
                                if isinstance(loc, dict):
                                    ville = loc.get("ville", "").strip()

                                    if ville:  # Only add the location if 'ville' is not empty
                                        job_locations.append({
                                            "ville": ville
                                        })

        logger.info(f"Transformed {len(job_locations)} job locations.")
        return job_locations
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def display_results(job_locations):
    try:
        logger.info("Job locations extracted and transformed:")
        for location in job_locations:
            print(location)  # Output the result to the console
    except Exception as e:
        logger.error(f"Error displaying results: {e}")
        raise

def main():
    try:
        # Extraction des données depuis MongoDB (dossier et frontusers)
        mongo_data = extract_from_mongodb()
        
        # Transformation des données extraites
        job_locations = transform_data(mongo_data)
        
        # Affichage des résultats dans la console
        display_results(job_locations)

    except Exception as e:
        logger.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
