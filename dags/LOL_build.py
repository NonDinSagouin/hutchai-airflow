import sys
import os
import logging
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, Variable

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.extraction as extraction
import app.tasks.load as load
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_build"
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
    def rename_columns_df(xcom_source: str, columns_mapping: dict, **context) -> pd.DataFrame:
        """Renomme les colonnes d'un DataFrame selon un mapping donné.

        Args:
            xcom_source (str): Tâche source pour récupérer le DataFrame depuis XCom.
            columns_mapping (dict): Dictionnaire de mapping des noms de colonnes.

        Returns:
            pd.DataFrame: DataFrame avec les colonnes renommées.
        """
        # Récupération du DataFrame depuis XCom
        df: pd.DataFrame = manager.Xcom.get(xcom_source=xcom_source, **context)

        # Renommage des colonnes
        df_renamed = df.rename(columns=columns_mapping)

        return manager.Xcom.put(input=df_renamed, **context)

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

    TYPE_TIMESTAMP = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"

    task_create_fact_match_datas = load.Warehouse.create_table(
        task_id="task_create_fact_match_datas",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        schema="lol_datas",
        table_name="lol_fact_match_datas",
        columns={
            "match_id": "VARCHAR(50) PRIMARY KEY",
            "game_creation": "BIGINT DEFAULT NULL",
            "game_duration": "BIGINT DEFAULT NULL",
            "game_mode": "VARCHAR(50) DEFAULT NULL",
            "game_type": "VARCHAR(50) DEFAULT NULL",
            "game_version": "VARCHAR(20) DEFAULT NULL",
            "game_in_progress": "BOOLEAN DEFAULT FALSE",
            "is_processed": "BOOLEAN DEFAULT FALSE",
            "tech_date_creation": TYPE_TIMESTAMP,
            "tech_date_modification": TYPE_TIMESTAMP,
        },
    )
    task_create_fact_puuid = load.Warehouse.create_table(
        task_id="task_create_fact_puuid",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        schema="lol_datas",
        table_name="lol_fact_puuid",
        columns={
            "puuid": "VARCHAR(250) PRIMARY KEY",
            "game_name": "VARCHAR(100) DEFAULT NULL",
            "tag_line": "VARCHAR(10) DEFAULT NULL",
            "date_processed": "TIMESTAMP DEFAULT NULL",
            "tech_date_creation": TYPE_TIMESTAMP,
            "tech_date_modification": TYPE_TIMESTAMP,
        },
    )

    task_get_match_ids = extraction.Api_providers.get(
        conn_id="API_LOL_riot",
        task_id="task_get_match_ids",
        endpoint="/riot/account/v1/accounts/by-riot-id/JeanPomme/POMM",
        headers={"X-Riot-Token": Variable.get("LOL_Riot-Token")},
    )

    task_rename_columns_df = Custom.rename_columns_df(
        task_id="task_rename_columns_df",
        xcom_source="task_get_match_ids",
        columns_mapping={
            "gameName": "game_name",
            "tagLine": "tag_line",
        },
    )

    task_insert_fact_puuid = load.Warehouse.insert(
        task_id="task_insert_fact_puuid",
        xcom_source="task_rename_columns_df",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid",
        schema="lol_datas",
        if_exists="ignore",
    )

    chain(
        task_create_fact_match_datas,
        task_create_fact_puuid,
        task_get_match_ids,
        task_rename_columns_df,
        task_insert_fact_puuid,
    )
