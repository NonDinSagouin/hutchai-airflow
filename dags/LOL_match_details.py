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
SCHEDULE=timedelta(minutes=2, seconds=10)  # 2min après la fin de chaque run
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
        },
        limit=90,
    )

    task_fetch_match_details = extraction.Api_riotgames.fetch_match_details(
        task_id = "task_fetch_match_details",
        xcom_source="task_get_matchs",
    )

    with TaskGroup("groupe_match_datas") as groupe_match_datas:

        task_extract_match_data = load.Warehouse.extract_from_dict(
            task_id="task_extract_match_data",
            xcom_source="task_fetch_match_details",
            key = "match_data",
        )

        task_drop_duplicates_match_data = transformation.Clean.drop_duplicates(
            task_id="task_drop_duplicates_match_data",
            xcom_source="groupe_match_datas.task_extract_match_data",
            subset_columns=["match_id"],
        )

        task_insert_lol_raw_match_data = load.Warehouse.insert(
            task_id="task_insert_lol_raw_match_data",
            xcom_source="groupe_match_datas.task_drop_duplicates_match_data",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_match_datas",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        task_raw_to_fact_match_datas = load.Warehouse.raw_to_fact(
            task_id="task_raw_to_fact",
            outlets=[Asset('warehouse://lol_datas/lol_fact_match_datas')],
            source_table="lol_datas.lol_raw_match_datas",
            target_table="lol_datas.lol_fact_match_datas",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=False,
            has_matched=True,
            join_keys=["match_id"],
            match_columns={
                "game_creation": "game_creation",
                "game_duration": "game_duration",
                "game_mode": "game_mode",
                "game_version": "game_version",
                "game_in_progress": "game_in_progress",
                "is_processed": "true",
            },
        )

        chain(
            task_extract_match_data,
            task_drop_duplicates_match_data,
            task_insert_lol_raw_match_data,
            task_raw_to_fact_match_datas,
        )

    with TaskGroup("groupe_stats") as groupe_stats:

        task_extract_stats = load.Warehouse.extract_from_dict(
            task_id="task_extract_stats",
            xcom_source="task_fetch_match_details",
            key = "stats_participants",
        )

        task_drop_duplicates_stats = transformation.Clean.drop_duplicates(
            task_id="task_drop_duplicates_stats",
            xcom_source="groupe_stats.task_extract_stats",
            subset_columns=["id"],
        )

        task_insert_lol_raw_stats = load.Warehouse.insert(
            task_id="task_insert_lol_raw_stats",
            xcom_source="groupe_stats.task_drop_duplicates_stats",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_stats",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )
        task_raw_to_fact_stats = load.Warehouse.raw_to_fact(
            task_id="task_raw_to_fact",
            outlets=[Asset('warehouse://lol_datas/lol_fact_stats')],
            source_table="lol_datas.lol_raw_stats",
            target_table="lol_datas.lol_fact_stats",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=False,
            join_keys=["match_id"],
            non_match_columns={
                'id': 'id',
                'match_id': 'match_id',
                'puuid': 'puuid',
                'game_creation': 'game_creation',
                'game_version': 'game_version',
                'game_mode': 'game_mode',
                'champion_id': 'champion_id',
                'champion_name': 'champion_name',
                'kills': 'kills',
                'deaths': 'deaths',
                'assists': 'assists',
                'kda': 'kda',
                'double_kills': 'double_kills',
                'triple_kills': 'triple_kills',
                'quadra_kills': 'quadra_kills',
                'penta_kills': 'penta_kills',
                'largest_killing_spree': 'largest_killing_spree',
                'total_damage_dealt': 'total_damage_dealt',
                'total_damage_dealt_to_champions': 'total_damage_dealt_to_champions',
                'physical_damage_dealt_to_champions': 'physical_damage_dealt_to_champions',
                'magic_damage_dealt_to_champions': 'magic_damage_dealt_to_champions',
                'true_damage_dealt_to_champions': 'true_damage_dealt_to_champions',
                'largest_critical_strike': 'largest_critical_strike',
                'total_damage_taken': 'total_damage_taken',
                'physical_damage_taken': 'physical_damage_taken',
                'magic_damage_taken': 'magic_damage_taken',
                'true_damage_taken': 'true_damage_taken',
                'total_heal': 'total_heal',
                'total_heals_on_teammates': 'total_heals_on_teammates',
                'total_minions_killed': 'total_minions_killed',
                'neutral_minions_killed': 'neutral_minions_killed',
                'gold_earned': 'gold_earned',
                'champ_level': 'champ_level',
                'champ_experience': 'champ_experience',
            },
        )
        chain(
            task_extract_stats,
            task_drop_duplicates_stats,
            task_insert_lol_raw_stats,
            task_raw_to_fact_stats,
        )

    chain(
        task_get_matchs,
        task_fetch_match_details,
        groupe_stats,
        groupe_match_datas,
    )