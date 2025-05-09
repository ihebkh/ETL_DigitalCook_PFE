import psycopg2

pays_data = [
    ("Afghanistan", "Afghanistan", "AF"),
    ("Albanie", "Albania", "AL"),
    ("Algérie", "Algeria", "DZ"),
    ("Andorre", "Andorra", "AD"),
    ("Angola", "Angola", "AO"),
    ("Antigua-et-Barbuda", "Antigua and Barbuda", "AG"),
    ("Argentine", "Argentina", "AR"),
    ("Arménie", "Armenia", "AM"),
    ("Australie", "Australia", "AU"),
    ("Autriche", "Austria", "AT"),
    ("Azerbaïdjan", "Azerbaijan", "AZ"),
    ("Bahamas", "Bahamas", "BS"),
    ("Bahreïn", "Bahrain", "BH"),
    ("Bangladesh", "Bangladesh", "BD"),
    ("Barbade", "Barbados", "BB"),
    ("Belgique", "Belgium", "BE"),
    ("Bélarus", "Belarus", "BY"),
    ("Belize", "Belize", "BZ"),
    ("Bénin", "Benin", "BJ"),
    ("Bhoutan", "Bhutan", "BT"),
    ("Bolivie", "Bolivia", "BO"),
    ("Bosnie-Herzégovine", "Bosnia and Herzegovina", "BA"),
    ("Botswana", "Botswana", "BW"),
    ("Brésil", "Brazil", "BR"),
    ("Brunei", "Brunei", "BN"),
    ("Bulgarie", "Bulgaria", "BG"),
    ("Burkina Faso", "Burkina Faso", "BF"),
    ("Burundi", "Burundi", "BI"),
    ("Cambodge", "Cambodia", "KH"),
    ("Cameroun", "Cameroon", "CM"),
    ("Canada", "Canada", "CA"),
    ("Cap-Vert", "Cape Verde", "CV"),
    ("Chili", "Chile", "CL"),
    ("Chine", "China", "CN"),
    ("Chypre", "Cyprus", "CY"),
    ("Colombie", "Colombia", "CO"),
    ("Comores", "Comoros", "KM"),
    ("Congo (République du)", "Congo (Republic)", "CG"),
    ("Congo (République démocratique du)", "Congo (Democratic Republic)", "CD"),
    ("Costa Rica", "Costa Rica", "CR"),
    ("Croatie", "Croatia", "HR"),
    ("Cuba", "Cuba", "CU"),
    ("Danemark", "Denmark", "DK"),
    ("Djibouti", "Djibouti", "DJ"),
    ("Dominique", "Dominica", "DM"),
    ("Égypte", "Egypt", "EG"),
    ("El Salvador", "El Salvador", "SV"),
    ("Équateur", "Ecuador", "EC"),
    ("Espagne", "Spain", "ES"),
    ("Estonie", "Estonia", "EE"),
    ("États-Unis", "United States", "US"),
    ("Éthiopie", "Ethiopia", "ET"),
    ("Fidji", "Fiji", "FJ"),
    ("Finlande", "Finland", "FI"),
    ("France", "France", "FR"),
    ("Gabon", "Gabon", "GA"),
    ("Gambie", "Gambia", "GM"),
    ("Géorgie", "Georgia", "GE"),
    ("Ghana", "Ghana", "GH"),
    ("Grèce", "Greece", "GR"),
    ("Grenade", "Grenada", "GD"),
    ("Guatemala", "Guatemala", "GT"),
    ("Guinée", "Guinea", "GN"),
    ("Guinée-Bissau", "Guinea-Bissau", "GW"),
    ("Guyana", "Guyana", "GY"),
    ("Haïti", "Haiti", "HT"),
    ("Honduras", "Honduras", "HN"),
    ("Hongrie", "Hungary", "HU"),
    ("Îles Marshall", "Marshall Islands", "MH"),
    ("Inde", "India", "IN"),
    ("Indonésie", "Indonesia", "ID"),
    ("Irak", "Iraq", "IQ"),
    ("Irlande", "Ireland", "IE"),
    ("Israël", "Israel", "IL"),
    ("Italie", "Italy", "IT"),
    ("Jamaïque", "Jamaica", "JM"),
    ("Japon", "Japan", "JP"),
    ("Jordanie", "Jordan", "JO"),
    ("Kazakhstan", "Kazakhstan", "KZ"),
    ("Kenya", "Kenya", "KE"),
    ("Kirghizistan", "Kyrgyzstan", "KG"),
    ("Koweït", "Kuwait", "KW"),
    ("Laos", "Laos", "LA"),
    ("Lesotho", "Lesotho", "LS"),
    ("Lettonie", "Latvia", "LV"),
    ("Liban", "Lebanon", "LB"),
    ("Libye", "Libya", "LY"),
    ("Luxembourg", "Luxembourg", "LU"),
    ("Macédoine du Nord", "North Macedonia", "MK"),
    ("Madagascar", "Madagascar", "MG"),
    ("Malaisie", "Malaysia", "MY"),
    ("Malawi", "Malawi", "MW"),
    ("Malte", "Malta", "MT"),
    ("Maroc", "Morocco", "MA"),
    ("Maurice", "Mauritius", "MU"),
    ("Mauritanie", "Mauritania", "MR"),
    ("Mexique", "Mexico", "MX"),
    ("Micronésie", "Micronesia", "FM"),
    ("Moldavie", "Moldova", "MD"),
    ("Monaco", "Monaco", "MC"),
    ("Mongolie", "Mongolia", "MN"),
    ("Mozambique", "Mozambique", "MZ"),
    ("Namibie", "Namibia", "NA"),
    ("Népal", "Nepal", "NP"),
    ("Nicaragua", "Nicaragua", "NI"),
    ("Niger", "Niger", "NE"),
    ("Nigeria", "Nigeria", "NG"),
    ("Norvège", "Norway", "NO"),
    ("Nouvelle-Zélande", "New Zealand", "NZ"),
    ("Oman", "Oman", "OM"),
    ("Ouganda", "Uganda", "UG"),
    ("Ouzbékistan", "Uzbekistan", "UZ"),
    ("Pakistan", "Pakistan", "PK"),
    ("Panama", "Panama", "PA"),
    ("Pérou", "Peru", "PE"),
    ("Philippines", "Philippines", "PH"),
    ("Pologne", "Poland", "PL"),
    ("Portugal", "Portugal", "PT"),
    ("Qatar", "Qatar", "QA"),
    ("République dominicaine", "Dominican Republic", "DO"),
    ("République tchèque", "Czech Republic", "CZ"),
    ("Roumanie", "Romania", "RO"),
    ("Royaume-Uni", "United Kingdom", "GB"),
    ("Russie", "Russia", "RU"),
    ("Rwanda", "Rwanda", "RW"),
    ("Sénégal", "Senegal", "SN"),
    ("Serbie", "Serbia", "RS"),
    ("Singapour", "Singapore", "SG"),
    ("Sri Lanka", "Sri Lanka", "LK"),
    ("Suède", "Sweden", "SE"),
    ("Suisse", "Switzerland", "CH"),
    ("Syrie", "Syria", "SY"),
    ("Tanzanie", "Tanzania", "TZ"),
    ("Thaïlande", "Thailand", "TH"),
    ("Tunisie", "Tunisia", "TN"),
    ("Turquie", "Turkey", "TR"),
    ("Ukraine", "Ukraine", "UA"),
    ("Uruguay", "Uruguay", "UY"),
    ("Venezuela", "Venezuela", "VE"),
    ("Vietnam", "Vietnam", "VN"),
    ("Zambie", "Zambia", "ZM"),
    ("Zimbabwe", "Zimbabwe", "ZW")
]

def get_postgresql_connection():
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
        print(f"Erreur de connexion à la base de données: {e}")
        raise

def insert_pays():
    conn = get_postgresql_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO dim_pays (code_pays, nom_pays_fr, nom_pays_en) 
    VALUES (%s, %s, %s)
    ON CONFLICT (code_pays) DO NOTHING;
    """

    for pays in pays_data:
        cur.execute(insert_query, (pays[2], pays[0], pays[1]))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"{len(pays_data)} pays insérés ou ignorés si déjà présents.")

if __name__ == "__main__":
    insert_pays()
