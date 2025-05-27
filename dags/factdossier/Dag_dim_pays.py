import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import contextmanager
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pays_data = [
    ("Afghanistan", "AF"), ("Albania", "AL"), ("Algeria", "DZ"), ("Andorra", "AD"),
    ("Angola", "AO"), ("Antigua and Barbuda", "AG"), ("Argentina", "AR"), ("Armenia", "AM"),
    ("Australia", "AU"), ("Austria", "AT"), ("Azerbaijan", "AZ"), ("Bahamas", "BS"),
    ("Bahrain", "BH"), ("Bangladesh", "BD"), ("Barbados", "BB"), ("Belgium", "BE"),
    ("Belarus", "BY"), ("Belize", "BZ"), ("Benin", "BJ"), ("Bhutan", "BT"),
    ("Bolivia", "BO"), ("Bosnia and Herzegovina", "BA"), ("Botswana", "BW"), ("Brazil", "BR"),
    ("Brunei", "BN"), ("Bulgaria", "BG"), ("Burkina Faso", "BF"), ("Burundi", "BI"),
    ("Cambodia", "KH"), ("Cameroon", "CM"), ("Canada", "CA"), ("Cape Verde", "CV"),
    ("Chile", "CL"), ("China", "CN"), ("Cyprus", "CY"), ("Colombia", "CO"),
    ("Comoros", "KM"), ("Republic of the Congo", "CG"), ("Democratic Republic of the Congo", "CD"),
    ("Costa Rica", "CR"), ("Croatia", "HR"), ("Cuba", "CU"), ("Denmark", "DK"),
    ("Djibouti", "DJ"), ("Dominica", "DM"), ("Egypt", "EG"), ("El Salvador", "SV"),
    ("Ecuador", "EC"), ("Spain", "ES"), ("Estonia", "EE"), ("United States", "US"),
    ("Ethiopia", "ET"), ("Fiji", "FJ"), ("Finland", "FI"), ("France", "FR"),
    ("Gabon", "GA"), ("Germany", "DE"), ("Gambia", "GM"), ("Georgia", "GE"),
    ("Ghana", "GH"), ("Greece", "GR"), ("Grenada", "GD"), ("Guatemala", "GT"),
    ("Guinea", "GN"), ("Guinea-Bissau", "GW"), ("Guyana", "GY"), ("Haiti", "HT"),
    ("Honduras", "HN"), ("Hungary", "HU"), ("India", "IN"), ("Indonesia", "ID"),
    ("Iraq", "IQ"), ("Ireland", "IE"), ("Israel", "IL"), ("Italy", "IT"),
    ("Jamaica", "JM"), ("Japan", "JP"), ("Jordan", "JO"), ("Kazakhstan", "KZ"),
    ("Kenya", "KE"), ("Kyrgyzstan", "KG"), ("Kuwait", "KW"), ("Laos", "LA"),
    ("Lesotho", "LS"), ("Latvia", "LV"), ("Lebanon", "LB"), ("Libya", "LY"),
    ("Luxembourg", "LU"), ("North Macedonia", "MK"), ("Madagascar", "MG"),
    ("Malaysia", "MY"), ("Malawi", "MW"), ("Malta", "MT"), ("Morocco", "MA"),
    ("Mauritius", "MU"), ("Mauritania", "MR"), ("Mexico", "MX"), ("Micronesia", "FM"),
    ("Moldova", "MD"), ("Monaco", "MC"), ("Mongolia", "MN"), ("Mozambique", "MZ"),
    ("Namibia", "NA"), ("Nepal", "NP"), ("Nicaragua", "NI"), ("Niger", "NE"),
    ("Nigeria", "NG"), ("Norway", "NO"), ("New Zealand", "NZ"), ("Oman", "OM"),
    ("Uganda", "UG"), ("Uzbekistan", "UZ"), ("Pakistan", "PK"), ("Panama", "PA"),
    ("Peru", "PE"), ("Philippines", "PH"), ("Poland", "PL"), ("Portugal", "PT"),
    ("Qatar", "QA"), ("Dominican Republic", "DO"), ("Czech Republic", "CZ"),
    ("Romania", "RO"), ("United Kingdom", "GB"), ("Russia", "RU"), ("Rwanda", "RW"),
    ("Senegal", "SN"), ("Serbia", "RS"), ("Singapore", "SG"), ("Sri Lanka", "LK"),
    ("Sweden", "SE"), ("Switzerland", "CH"), ("Syria", "SY"), ("Tanzania", "TZ"),
    ("Thailand", "TH"), ("Tunisia", "TN"), ("Turkey", "TR"), ("Ukraine", "UA"),
    ("Uruguay", "UY"), ("Venezuela", "VE"), ("Vietnam", "VN"), ("Zambia", "ZM"),
    ("Zimbabwe", "ZW"), ("Saudi Arabia", "SA")
]

@contextmanager
def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    try:
        yield conn
    finally:
        conn.close()

def extract_pays(**kwargs):
    kwargs['ti'].xcom_push(key='pays_data', value=pays_data)
    logger.info(f"{len(pays_data)} pays extraits pour mise à jour.")

def insert_pays_to_postgres(**kwargs):
    try:
        pays_list = kwargs['ti'].xcom_pull(task_ids='extract_pays', key='pays_data')
        if not pays_list:
            logger.info("Aucun pays à insérer.")
            return

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO dim_pays (nom_pays_en, code_pays)
                VALUES (%s, %s)
                ON CONFLICT (code_pays) DO UPDATE
                SET nom_pays_en = EXCLUDED.nom_pays_en;
                """
                for pays in pays_list:
                    cur.execute(insert_query, pays)
                conn.commit()

        logger.info(f"{len(pays_list)} pays insérés/mis à jour.")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des pays : {e}")
        raise

with DAG(
    dag_id='Dag_dim_pays',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logger.info("🔹 Début du DAG d'insertion/mise à jour des pays.")
    )

    extract_task = PythonOperator(
        task_id='extract_pays',
        python_callable=extract_pays,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='insert_pays_to_postgres',
        python_callable=insert_pays_to_postgres,
        provide_context=True
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=lambda: logger.info("✅ Fin du DAG d'insertion/mise à jour des pays.")
    )

    start_task >> extract_task >> load_task >> end_task
