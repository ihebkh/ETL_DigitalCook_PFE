import psycopg2

def get_postgres_cursor():
    try:
        # Connexion à la base de données PostgreSQL sur Azure
        conn = psycopg2.connect(
            dbname="DW_DigitalCook",
            user="iheb",
            password="201JmT1896@",
            host="monserveur-postgres.postgres.database.azure.com",
            port="5432"
        )
        # Création du curseur pour exécuter des requêtes
        cursor = conn.cursor()
        return cursor, conn
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return None, None

def get_tables_and_columns():
    # Récupère le curseur et la connexion
    cursor, conn = get_postgres_cursor()

    if cursor:
        try:
            # Exécution de la requête pour récupérer toutes les tables et leurs colonnes
            query = """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            # Organiser les résultats par table et ses colonnes
            tables = {}
            for row in rows:
                table_name, column_name = row
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append(column_name)
            
            return tables
        except Exception as e:
            print(f"Erreur lors de l'exécution de la requête : {e}")
        finally:
            # Fermeture du curseur et de la connexion
            cursor.close()
            conn.close()
    else:
        print("Impossible de se connecter à la base de données.")
        return {}

# Exemple d'utilisation : récupérer toutes les tables et leurs colonnes
tables_and_columns = get_tables_and_columns()

# Affichage des résultats
for table, columns in tables_and_columns.items():
    print(f"Table: {table}")
    for column in columns:
        print(f"  Column: {column}")
