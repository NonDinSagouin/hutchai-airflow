import sys
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, TaskGroup, Asset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.api as api
import app.tasks.databases as databases
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_enrich_fact_stats"
DESCRIPTION = "Récupération des statistiques des matchs des joueurs League of Legends via l'API Riot Games et stockage dans l'entrepôt de données."
OBJECTIF = " Ce DAG vise à extraire les identifiants des matchs des joueurs League of Legends depuis une table factuelle, " \
"interroger l'API Riot Games pour obtenir les détails de chaque match," \
"extraire et stocker les statistiques des participants dans une table factuelle dédiée dans l'entrepôt de données."
REMARQUE = "Assurez-vous que les clés API Riot Games sont correctement configurées dans le gestionnaire de connexions avant d'exécuter ce DAG." \
" Si il n'y a pas de nouvelles données à traiter, le DAG skipera les étapes inutiles."
SCHEDULE = "*/3 10-18 * * *" # Tous les jours toutes les 3 minutes entre 10h et 18h
START_DATE = datetime(2025, 1, 1)
TAGS = []

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=10), # Temps entre chaque tentative
}

# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=START_DATE, # Date de début du DAG
    schedule=SCHEDULE,  # Fréquence d'exécution (CRON ou timedelta)
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

        ## 🔹 Remarque
        {REMARQUE}
    """,
) as dag:

    # Extraction des matchs depuis la table factuelle
    get_matchs = databases.PostgresWarehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_match",
        schema="lol_fact_datas",
        task_id="get_matchs",
        schema_select={"match_id",},
        schema_where={
            "is_processed": "is false",
        },
        schema_order="tech_date_creation ASC",
        limit=200,
        skip_empty=True,
    )

    # Récupération des détails des matchs via l'API Riot Games
    fetch_match_details = api.Riotgames.fetch_match_details(
        task_id = "fetch_match_details",
        xcom_source="get_matchs",
    )

    # Extraction des statistiques des participants depuis les détails des matchs
    with TaskGroup("groupe_stats") as groupe_stats:

        # Extraction des statistiques des participants depuis les détails des matchs
        extract_stats = databases.PostgresWarehouse.extract_from_dict(
            task_id="extract_stats",
            xcom_source="fetch_match_details",
            key = "stats_participants",
        )

        # Suppression des doublons éventuels
        drop_duplicates_stats = transformation.Clean.drop_duplicates(
            task_id="drop_duplicates_stats",
            xcom_source="groupe_stats.extract_stats",
            subset_columns=["id"],
        )

        # Insertion des données brutes dans la table d'entrepôt
        insert_lol_raw_stats = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_stats",
            xcom_source="groupe_stats.drop_duplicates_stats",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_stats",
            schema="lol_raw_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        # Transformation des données brutes en données factuelles
        raw_to_fact_stats = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact",
            outlets=[Asset('warehouse://lol_fact_datas/lol_fact_stats')],
            source_table="lol_raw_datas.lol_raw_stats",
            target_table="lol_fact_datas.lol_fact_stats",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=False,
            join_keys=["match_id"],
            non_match_columns={
                'id': 'id',
                'match_id': 'match_id',
                'puuid': 'puuid',
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
            extract_stats,
            drop_duplicates_stats,
            insert_lol_raw_stats,
            raw_to_fact_stats,
        )

    # Extraction des PUUIDs depuis les détails des matchs
    with TaskGroup("groupe_puuid") as groupe_puuid:

        # Extraction des PUUIDs des participants depuis les détails des matchs
        extract_puuid_participants = databases.PostgresWarehouse.extract_from_dict(
            task_id="extract_puuid_participants",
            xcom_source="fetch_match_details",
            key = "puuid_participants",
        )

        # Suppression des doublons éventuels
        drop_duplicates_stats = transformation.Clean.drop_duplicates(
            task_id="drop_duplicates_stats",
            xcom_source="groupe_puuid.extract_puuid_participants",
            subset_columns=["puuid"],
        )

        # Insertion des données brutes dans la table d'entrepôt
        insert_lol_raw_puuid = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_puuid",
            xcom_source="groupe_puuid.drop_duplicates_stats",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_puuid",
            schema="lol_raw_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        # Transformation des données brutes en données factuelles
        raw_to_fact_matchs = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact",
            outlets=[Asset('warehouse://lol_fact_datas/lol_fact_puuid')],
            source_table="lol_raw_datas.lol_raw_puuid",
            target_table="lol_fact_datas.lol_fact_puuid",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=False,
            join_keys=["puuid"],
            non_match_columns={
                "puuid": "puuid",
            },
        )

        chain(extract_puuid_participants, drop_duplicates_stats, insert_lol_raw_puuid, raw_to_fact_matchs)

    # Extraction et traitement des matchs complets
    with TaskGroup("groupe_match") as groupe_match:

        # Extraction des matchs complets depuis les détails des matchs
        extract_match = databases.PostgresWarehouse.extract_from_dict(
            task_id="extract_match",
            xcom_source="fetch_match_details",
            key = "match_data",
        )

        # Suppression des doublons éventuels
        drop_duplicates_match = transformation.Clean.drop_duplicates(
            task_id="drop_duplicates_match",
            xcom_source="groupe_match.extract_match",
            subset_columns=["match_id"],
        )

        # Insertion des données brutes dans la table d'entrepôt
        insert_lol_raw_match = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_match",
            xcom_source="groupe_match.drop_duplicates_match",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_match",
            schema="lol_raw_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        # Transformation des données brutes en données factuelles
        raw_to_fact_match = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact_match",
            outlets=[Asset('warehouse://lol_fact_datas/lol_fact_match')],
            source_table="lol_raw_datas.lol_raw_match",
            target_table="lol_fact_datas.lol_fact_match",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=False,
            has_matched=True,
            join_keys=["match_id"],
            match_columns={
                "puuid_1": "puuid_1",
                "puuid_2": "puuid_2",
                "puuid_3": "puuid_3",
                "puuid_4": "puuid_4",
                "puuid_5": "puuid_5",
                "puuid_6": "puuid_6",
                "puuid_7": "puuid_7",
                "puuid_8": "puuid_8",
                "puuid_9": "puuid_9",
                "puuid_10": "puuid_10",
                "game_creation": "game_creation",
                "game_duration": "game_duration",
                "game_mode": "game_mode",
                "game_version": "game_version",
                "game_in_progress": "game_in_progress",
                "is_processed": "true",
            },
        )

        chain(
            extract_match,
            drop_duplicates_match,
            insert_lol_raw_match,
            raw_to_fact_match,
        )

    # Définition de l'ordre d'exécution des tâches
    chain(
        get_matchs,
        fetch_match_details,
        [groupe_stats, groupe_puuid,],
        groupe_match,
    )