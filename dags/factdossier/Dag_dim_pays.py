import psycopg2

pays_data = [
    ("Afghanistan", "AF"),
    ("Albania", "AL"),
    ("Algeria", "DZ"),
    ("Andorra", "AD"),
    ("Angola", "AO"),
    ("Antigua and Barbuda", "AG"),
    ("Argentina", "AR"),
    ("Armenia", "AM"),
    ("Australia", "AU"),
    ("Austria", "AT"),
    ("Azerbaijan", "AZ"),
    ("Bahamas", "BS"),
    ("Bahrain", "BH"),
    ("Bangladesh", "BD"),
    ("Barbados", "BB"),
    ("Belgium", "BE"),
    ("Belarus", "BY"),
    ("Belize", "BZ"),
    ("Benin", "BJ"),
    ("Bhutan", "BT"),
    ("Bolivia", "BO"),
    ("Bosnia and Herzegovina", "BA"),
    ("Botswana", "BW"),
    ("Brazil", "BR"),
    ("Brunei", "BN"),
    ("Bulgaria", "BG"),
    ("Burkina Faso", "BF"),
    ("Burundi", "BI"),
    ("Cambodia", "KH"),
    ("Cameroon", "CM"),
    ("Canada", "CA"),
    ("Cape Verde", "CV"),
    ("Chile", "CL"),
    ("China", "CN"),
    ("Cyprus", "CY"),
    ("Colombia", "CO"),
    ("Comoros", "KM"),
    ("Republic of the Congo", "CG"),
    ("Democratic Republic of the Congo", "CD"),
    ("Costa Rica", "CR"),
    ("Croatia", "HR"),
    ("Cuba", "CU"),
    ("Denmark", "DK"),
    ("Djibouti", "DJ"),
    ("Dominica", "DM"),
    ("Egypt", "EG"),
    ("El Salvador", "SV"),
    ("Ecuador", "EC"),
    ("Spain", "ES"),
    ("Estonia", "EE"),
    ("United States", "US"),
    ("Ethiopia", "ET"),
    ("Fiji", "FJ"),
    ("Finland", "FI"),
    ("France", "FR"),
    ("Gabon", "GA"),
    ("Germany", "DE"),
    ("Gambia", "GM"),
    ("Georgia", "GE"),
    ("Ghana", "GH"),
    ("Greece", "GR"),
    ("Grenada", "GD"),
    ("Guatemala", "GT"),
    ("Guinea", "GN"),
    ("Guinea-Bissau", "GW"),
    ("Guyana", "GY"),
    ("Haiti", "HT"),
    ("Honduras", "HN"),
    ("Hungary", "HU"),
    ("Marshall Islands", "MH"),
    ("India", "IN"),
    ("Indonesia", "ID"),
    ("Iraq", "IQ"),
    ("Ireland", "IE"),
    ("Israel", "IL"),
    ("Italy", "IT"),
    ("Jamaica", "JM"),
    ("Japan", "JP"),
    ("Jordan", "JO"),
    ("Kazakhstan", "KZ"),
    ("Kenya", "KE"),
    ("Kyrgyzstan", "KG"),
    ("Kuwait", "KW"),
    ("Laos", "LA"),
    ("Lesotho", "LS"),
    ("Latvia", "LV"),
    ("Lebanon", "LB"),
    ("Libya", "LY"),
    ("Luxembourg", "LU"),
    ("North Macedonia", "MK"),
    ("Madagascar", "MG"),
    ("Malaysia", "MY"),
    ("Malawi", "MW"),
    ("Malta", "MT"),
    ("Morocco", "MA"),
    ("Mauritius", "MU"),
    ("Mauritania", "MR"),
    ("Mexico", "MX"),
    ("Micronesia", "FM"),
    ("Moldova", "MD"),
    ("Monaco", "MC"),
    ("Mongolia", "MN"),
    ("Mozambique", "MZ"),
    ("Namibia", "NA"),
    ("Nepal", "NP"),
    ("Nicaragua", "NI"),
    ("Niger", "NE"),
    ("Nigeria", "NG"),
    ("Norway", "NO"),
    ("New Zealand", "NZ"),
    ("Oman", "OM"),
    ("Uganda", "UG"),
    ("Uzbekistan", "UZ"),
    ("Pakistan", "PK"),
    ("Panama", "PA"),
    ("Peru", "PE"),
    ("Philippines", "PH"),
    ("Poland", "PL"),
    ("Portugal", "PT"),
    ("Qatar", "QA"),
    ("Dominican Republic", "DO"),
    ("Czech Republic", "CZ"),
    ("Romania", "RO"),
    ("United Kingdom", "GB"),
    ("Russia", "RU"),
    ("Rwanda", "RW"),
    ("Senegal", "SN"),
    ("Serbia", "RS"),
    ("Singapore", "SG"),
    ("Sri Lanka", "LK"),
    ("Sweden", "SE"),
    ("Switzerland", "CH"),
    ("Syria", "SY"),
    ("Tanzania", "TZ"),
    ("Thailand", "TH"),
    ("Tunisia", "TN"),
    ("Turkey", "TR"),
    ("Ukraine", "UA"),
    ("Uruguay", "UY"),
    ("Venezuela", "VE"),
    ("Vietnam", "VN"),
    ("Zambia", "ZM"),
    ("Zimbabwe", "ZW"),
    ("Saudi Arabia", "SA")

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
    INSERT INTO dim_pays (nom_pays_en, code_pays) 
    VALUES (%s, %s)
    ON CONFLICT (code_pays) DO NOTHING;
    """

    for pays in pays_data:
        cur.execute(insert_query, (pays[0], pays[1]))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"{len(pays_data)} pays insérés ou ignorés si déjà présents.")

if __name__ == "__main__":
    insert_pays()
