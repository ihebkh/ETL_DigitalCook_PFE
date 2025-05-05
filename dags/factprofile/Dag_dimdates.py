import pandas as pd
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_date_df(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame({
        'datecode': date_range,
        'Jour_Mois_Annee': date_range.strftime('%d-%m-%Y'),
        'Annee': date_range.year,
        'id_semestre': ((date_range.month - 1) // 6) + 1,
        'semestre': ((date_range.month - 1) // 6) + 1,
        'id_trimestre': ((date_range.month - 1) // 3) + 1,
        'trimestre': ((date_range.month - 1) // 3) + 1,
        'id_mois': date_range.month,
        'mois': date_range.month,
        'lib_mois': date_range.strftime('%B'),
        'id_jour': date_range.strftime('%A'),
        'jour': date_range.day,
        'lib_jour': date_range.strftime('%A'),
        'semaine': date_range.isocalendar().week,
        'JourDeAnnee': date_range.dayofyear,
        'Jour_mois_lettre': date_range.strftime('%d %B')
    })
    return df

def load_dim_dates_to_postgres(**kwargs):
    try:
        start_date = '1980-01-01'
        end_date = '2030-01-01'
        df = create_date_df(start_date, end_date)

        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO dim_Dates (
            code_date, "jour_mois_annee", "annee", id_semestre, semestre,
            id_trimestre, trimestre, id_mois, mois, libelle_mois,
            id_jour, jour, libelle_jour, semaine, "jour_de_annee", "jour_mois_lettre"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (code_date) DO UPDATE SET
            "code_date" = EXCLUDED."code_date",
            "jour_mois_annee" = EXCLUDED."jour_mois_annee",
            "annee" = EXCLUDED."annee",
            id_semestre = EXCLUDED.id_semestre,
            semestre = EXCLUDED.semestre,
            id_trimestre = EXCLUDED.id_trimestre,
            trimestre = EXCLUDED.trimestre,
            id_mois = EXCLUDED.id_mois,
            mois = EXCLUDED.mois,
            libelle_mois = EXCLUDED.libelle_mois,
            id_jour = EXCLUDED.id_jour,
            jour = EXCLUDED.jour,
            libelle_jour = EXCLUDED.libelle_jour,
            semaine = EXCLUDED.semaine,
            "jour_de_annee" = EXCLUDED."jour_de_annee",
            "jour_mois_lettre" = EXCLUDED."jour_mois_lettre"
        """

        data_tuples = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        logger.info(f"Successfully inserted/updated {len(data_tuples)} rows into dim_Dates.")
        cursor.close()
        conn.close()
        logger.info(f" {len(data_tuples)} lignes insérées/à jour dans dim_Dates.")

    except Exception as e:
        logger.error(f" Erreur lors du chargement dans dim_Dates : {e}")
        raise







dag = DAG(
    dag_id='dag_dim_dates',
    start_date=datetime(2025, 1, 1),
    catchup=False
)

load_task = PythonOperator(
    task_id='load_dim_dates',
    python_callable=load_dim_dates_to_postgres,
    provide_context=True,
    dag=dag
)