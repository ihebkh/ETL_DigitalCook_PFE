from pymongo import MongoClient
import psycopg2

def get_postgresql_connection():
    """Établit la connexion à PostgreSQL et retourne le connecteur."""
    try:
        conn = psycopg2.connect(
            dbname="DW_DigitalCook",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        raise

def get_mongodb_connection():
    """Établit la connexion à MongoDB et retourne le client et la collection nécessaire."""
    try:
        MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
        MONGO_DB = "PowerBi"
        UNIVERSITIES_COLLECTION = "universities"

        client = MongoClient(MONGO_URI)
        mongo_db = client[MONGO_DB]
        universities_collection = mongo_db[UNIVERSITIES_COLLECTION]

        return client, universities_collection

    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise

def get_ville_pk(ville_name):
    """Récupère le ville_pk de PostgreSQL à partir du nom de la ville."""
    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()

        query = """
            SELECT ville_pk
            FROM dim_ville
            WHERE nom_ville = %s
        """
        cursor.execute(query, (ville_name,))
        result = cursor.fetchone()

        if result:
            return result[0]
        return None

    except Exception as e:
        print(f"Error retrieving ville_pk from PostgreSQL: {e}")
        raise

    finally:
        cursor.close()

def get_filiere_pk(nomfiliere, domaine, diplome):
    """Récupère le filiere_pk de PostgreSQL à partir des informations de filière (nomfiliere, domaine, diplome)."""
    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()

        query = """
            SELECT filiere_pk
            FROM dim_filiere
            WHERE nomfiliere = %s AND domaine = %s AND diplome = %s
        """
        cursor.execute(query, (nomfiliere, domaine, diplome))
        result = cursor.fetchone()

        if result:
            return result[0]
        return None

    except Exception as e:
        print(f"Error retrieving filiere_pk from PostgreSQL: {e}")
        raise

    finally:
        cursor.close()

def get_contact_pk_from_postgres(firstname, lastname):
    """Récupère le contact_pk de PostgreSQL à partir du prénom et du nom de famille."""
    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()

        query = """
            SELECT contact_pk
            FROM public.dim_contact
            WHERE firstname = %s AND lastname = %s
        """
        cursor.execute(query, (firstname, lastname))
        result = cursor.fetchone()

        if result:
            return result[0]
        return None

    except Exception as e:
        print(f"Error retrieving contact_pk from PostgreSQL: {e}")
        raise

    finally:
        cursor.close()

def match_and_display_university_data():
    # Retrieve MongoDB connection and data
    client, universities_collection = get_mongodb_connection()

    # Extract universities data from MongoDB
    mongo_data = universities_collection.find({}, {
        "_id": 0,
        "nom": 1,                # University Name
        "pays": 1,               # Country
        "ville": 1,              # City
        "filiere": 1,            # Field of Study (filiere)
        "contact": 1,            # Contact details (for matching)
        "created_at": 1,         # Created Date
    })

    mongo_data_list = list(mongo_data)
    line_count = 0

    # Iterate over each university
    for university in mongo_data_list:
        # Extract fields for each university
        nom = university.get("nom", "Inconnu")
        pays = university.get("pays", "Inconnu")
        villes = university.get("ville", ["Inconnue"])  # Default to "Inconnue"
        filieres = university.get("filiere", [])  # List of filieres (fields of study)

        # Initialize lists for the current university
        city_pk_list = []
        contact_pk_list = []

        # Fetch city pk for each city in the "ville" field
        for ville in villes:
            city_pk = get_ville_pk(ville.strip().lower())
            # Fetch and print the city name with its pk
            print(f"Ville: {ville.strip()}, Ville PK: {city_pk}")
            city_pk_list.append(city_pk)

        # Fetch filiere pk for each field of study (filiere)
        filiere_pk_list = []
        for filiere in filieres:
            nomfiliere = filiere.get("nomfiliere", "").strip().lower()
            domaine = filiere.get("domaine", "").strip().lower()
            diplome = filiere.get("diplome", "").strip().lower()

            filiere_pk = get_filiere_pk(nomfiliere, domaine, diplome)
            filiere_pk_list.append(filiere_pk)

        # For each university, match and display or insert the corresponding information
        for i in range(max(len(city_pk_list), len(contact_pk_list), len(filiere_pk_list), 1)):
            # Get data or None if the list is shorter
            city_pk = city_pk_list[i] if i < len(city_pk_list) else None
            contact_pk = contact_pk_list[i] if i < len(contact_pk_list) else None
            filiere_pk = filiere_pk_list[i] if i < len(filiere_pk_list) else None

            # Print the university's details, including the city and its associated PK
            print(f"University: {nom}, Country: {pays}, City: {villes[i] if i < len(villes) else 'Inconnue'}, City PK: {city_pk}, Filiere PK: {filiere_pk}, Contact PK: {contact_pk}")

        line_count += 1

    print(f"\nTotal lines processed: {line_count}")

