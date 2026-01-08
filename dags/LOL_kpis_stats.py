import sys
import os
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.api as api
import app.tasks.databases as databases
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_kpis_stats"
DESCRIPTION = ""
OBJECTIF = ""
SCHEDULE = None
START_DATE = datetime(2025, 1, 1)
TAGS = []

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=10), # Temps entre chaque tentative
}

class Custom():

    @customTask
    @staticmethod
    def vide(**kwargs) -> None:
        """ Description de la fonction vide.
        Returns:
            None
        """
        spark = manager.Spark.get(
            app_name="LOL_KPI_DamagePerMinute",
            driver_memory="2g",
            sql_shuffle_partitions="4",
            **kwargs
        )

        logging.info("🔥 Début du calcul des KPI de dégâts par minute")

        postgres_engine = manager.Connectors.postgres("POSTGRES_warehouse")
        jdbc_url = postgres_engine.url.render_as_string(hide_password=False).replace("postgresql+psycopg2://", "jdbc:postgresql://")

        # Configuration JDBC pour PostgreSQL
        jdbc_properties = {
            "driver": "org.postgresql.Driver",
            "user": postgres_engine.url.username,
            "password": postgres_engine.url.password,
        }

        # Charger les données depuis PostgreSQL
        logging.info("📊 Chargement des données depuis PostgreSQL...")

        # Lire la table des PUUIDs à traiter
        df_puuid = spark.read.jdbc(
            url=jdbc_url,
            table="lol_fact_datas.lol_fact_puuid_to_process",
            properties=jdbc_properties
        )
        pandas_df = df_puuid.toPandas()
        print(pandas_df)


# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=START_DATE, # Date de début du DAG
    #schedule=SCHEDULE,  # Fréquence d'exécution (CRON ou timedelta)
    tags=TAGS, # Liste de tags pour catégoriser le DAG dans l'UI
    catchup=False, # Exécution des tâches manquées (True ou False)
    max_active_runs=1,  # Limite à 1 exécutions actives en même temps
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md=f"""
        ## 🔹 Description
        {DESCRIPTION}

        ## 🔹 Objectif
        {OBJECTIF}
    """,
) as dag:

    task_vide = Custom.vide(
        task_id = "task_vide",
    )

    chain(
        task_vide
    )
