from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime
import logging

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
        start_date = '2022-01-01'
        end_date = datetime.today().strftime('%Y-%m-%d')
        df = create_date_df(start_date, end_date)

        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO dim_Dates (
            datecode, "Jour_Mois_Annee", "Annee", id_semestre, semestre,
            id_trimestre, trimestre, id_mois, mois, lib_mois,
            id_jour, jour, lib_jour, semaine, "JourDeAnnee", "Jour_mois_lettre"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (datecode) DO UPDATE SET
            "Jour_Mois_Annee" = EXCLUDED."Jour_Mois_Annee",
            "Annee" = EXCLUDED."Annee",
            id_semestre = EXCLUDED.id_semestre,
            semestre = EXCLUDED.semestre,
            id_trimestre = EXCLUDED.id_trimestre,
            trimestre = EXCLUDED.trimestre,
            id_mois = EXCLUDED.id_mois,
            mois = EXCLUDED.mois,
            lib_mois = EXCLUDED.lib_mois,
            id_jour = EXCLUDED.id_jour,
            jour = EXCLUDED.jour,
            lib_jour = EXCLUDED.lib_jour,
            semaine = EXCLUDED.semaine,
            "JourDeAnnee" = EXCLUDED."JourDeAnnee",
            "Jour_mois_lettre" = EXCLUDED."Jour_mois_lettre"
        """

        data_tuples = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f" {len(data_tuples)} lignes insérées/à jour dans dim_Dates.")

    except Exception as e:
        logger.error(f" Erreur lors du chargement dans dim_Dates : {e}")
        raise



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}




dag = DAG(
    dag_id='dim_dates_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

load_task = PythonOperator(
    task_id='load_dim_dates',
    python_callable=load_dim_dates_to_postgres,
    provide_context=True,
    dag=dag
)
