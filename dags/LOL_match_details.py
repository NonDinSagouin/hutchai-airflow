import sys
import os
import pandas as pd
import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, TaskGroup, Asset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.extraction as extraction
import app.tasks.load as load
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_match_details"
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
    def vide(**context) -> None:
        """ Description de la fonction vide.
        Returns:
            None
        """
        pass

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

    task_get_matchs = load.Warehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_match_datas",
        schema="lol_datas",
        task_id="task_get_matchs",
        shema_select={"match_id",},
        shema_where={
            "is_processed": "false",
            "game_in_progress": "false",
        },
        limit=2, # 100
    )

    task_fetch_match_details = extraction.Api_riotgames.fetch_match_details(
        task_id = "task_fetch_match_details",
        xcom_source="task_get_matchs",
    )

    with TaskGroup("groupe_puuid") as groupe_puuid:

        task_extract_puuid_participants = load.Warehouse.extract_from_dict(
            task_id="task_extract_puuid_participants",
            xcom_source="task_fetch_match_details",
            key = "puuid_participants",
        )
        task_insert_lol_raw_puuid = load.Warehouse.insert(
            task_id="task_insert_lol_raw_puuid",
            xcom_source="groupe_puuid.task_extract_puuid_participants",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_puuid",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        task_raw_to_fact_matchs = load.Warehouse.raw_to_fact(
            task_id="task_raw_to_fact",
            outlets=[Asset('warehouse://lol_datas/lol_fact_puuid')],
            source_table="lol_datas.lol_raw_puuid",
            target_table="lol_datas.lol_fact_puuid",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=False,
            join_keys=["puuid"],
            non_match_columns={
                "puuid": "puuid",
            },
        )

        chain(task_extract_puuid_participants, task_insert_lol_raw_puuid, task_raw_to_fact_matchs)

    with TaskGroup("groupe_match_datas") as groupe_match_datas:

        task_extract_match_data = load.Warehouse.extract_from_dict(
            task_id="task_extract_match_data",
            xcom_source="task_fetch_match_details",
            key = "match_data",
        )
        task_insert_lol_raw_match_data = load.Warehouse.insert(
            task_id="task_insert_lol_raw_match_data",
            xcom_source="groupe_match_datas.task_extract_match_data",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_match_datas",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )
        chain(task_extract_match_data, task_insert_lol_raw_match_data)

    with TaskGroup("groupe_stats_participants") as groupe_stats_participants:

        task_extract_stats_participants = load.Warehouse.extract_from_dict(
            task_id="task_extract_stats_participants",
            xcom_source="task_fetch_match_details",
            key = "stats_participants",
        )
        task_insert_lol_raw_stats_participants = load.Warehouse.insert(
            task_id="task_insert_lol_raw_stats_participants",
            xcom_source="groupe_stats_participants.task_extract_stats_participants",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_stats_participants",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )
        chain(task_extract_stats_participants, task_insert_lol_raw_stats_participants)

    chain(
        task_get_matchs,
        task_fetch_match_details,
        [groupe_puuid, groupe_match_datas, groupe_stats_participants],
    )