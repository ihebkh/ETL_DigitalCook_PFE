import psycopg2

# Connexion à la base de données
conn = psycopg2.connect(
    dbname="DW_DigitalCook",
    user="postgres",
    password="admin",
    host="localhost",  # change si ce n’est pas local
    port="5432"        # change si besoin
)

# Création du curseur
cur = conn.cursor()

# Désactiver les contraintes de clé étrangère temporairement
cur.execute("SET session_replication_role = replica;")

# Récupérer toutes les tables de l'utilisateur courant
cur.execute("""
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public';
""")

tables = cur.fetchall()

# Supprimer les données de chaque table
for table in tables:
    print(f"🧹 Suppression du contenu de la table {table[0]}")
    cur.execute(f"TRUNCATE TABLE {table[0]} RESTART IDENTITY CASCADE;")

# Réactiver les contraintes
cur.execute("SET session_replication_role = DEFAULT;")

# Commit des changements
conn.commit()

# Fermeture
cur.close()
conn.close()

print("✅ Base vidée avec succès.")
