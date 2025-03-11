import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime

def create_date_df(start_date, end_date):
    date_range = pd.date_range(start_date, end_date)
    df = pd.DataFrame({
        'date': date_range,
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


try:
    conn = psycopg2.connect(
        dbname="DW_DigitalCook",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()


    start_date = '2022-01-01'
    end_date = datetime.today().strftime('%Y-%m-%d')
    date_df = create_date_df(start_date, end_date)

    data_tuples = [tuple(x) for x in date_df.to_numpy()]


    extras.execute_batch(cursor, """
        INSERT INTO dim_dates (
            date, Jour_Mois_Annee, Annee, id_semestre, semestre, id_trimestre, trimestre,
            id_mois, mois, lib_mois, id_jour, jour, lib_jour, semaine, JourDeAnnee, Jour_mois_lettre
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) 
        DO UPDATE SET 
            Jour_Mois_Annee = EXCLUDED.Jour_Mois_Annee,
            Annee = EXCLUDED.Annee,
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
            JourDeAnnee = EXCLUDED.JourDeAnnee,
            Jour_mois_lettre = EXCLUDED.Jour_mois_lettre
    """, data_tuples)

    conn.commit()
    print(" Table dimDates alimentée avec succès !")

except psycopg2.Error as e:
    print(f" Erreur PostgreSQL : {e}")
    conn.rollback()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
