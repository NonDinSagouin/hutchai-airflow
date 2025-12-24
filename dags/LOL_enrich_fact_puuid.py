import sys
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, Asset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.extraction as extraction
import app.tasks.load as load
import app.manager as manager
import app.library as library

DAG_ID = "LOL_enrich_fact_puuid"
DESCRIPTION = "Récupération des informations de PUUID des joueurs League of Legends via l'API Riot Games et stockage dans l'entrepôt de données."
OBJECTIF = "Ce DAG vise à extraire les PUUIDs des joueurs League of Legends depuis une table factuelle, " \
"interroger l'API Riot Games pour obtenir des informations détaillées sur chaque PUUID," \
"et stocker ces informations dans une table factuelle dédiée dans l'entrepôt de données."
REMARQUE = "Assurez-vous que les clés API Riot Games sont correctement configurées dans le gestionnaire de connexions avant d'exécuter ce DAG." \
" Si il n'y a pas de nouvelles données à traiter, le DAG skipera les étapes inutiles." \
" Ce DAG est programmé pour s'exécuter quotidiennement à 1h du matin."
SCHEDULE = "0 1 * * *"
START_DATE = datetime(2025, 1, 1)
TAGS = [library.TagsLibrary.LEAGUE_OF_LEGENDS, library.TagsLibrary.RIOT_GAMES, library.TagsLibrary.WAREHOUSE, library.TagsLibrary.DATA_ROWS, library.TagsLibrary.DATA_FACT]

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

    # Extraction des PUUIDs depuis la table factuelle
    task_get_puuid = load.Warehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid",
        schema="lol_fact_datas",
        task_id="task_get_puuid",
        schema_select={"puuid"},
        schema_where={
            "game_name": "is null",
            "tag_line": "is null",
        },
        schema_order="date_processed DESC",
        limit=100,
    )

    # Récupération des informations de PUUID via l'API Riot Games
    task_fetch_puuid_info = extraction.Api_riotgames.fetch_puuid_info(
        task_id="task_fetch_puuid_info",
        xcom_source='task_get_puuid',
    )

    # Insertion des données brutes dans la table d'entrepôt
    task_insert_raw_matchs = load.Warehouse.insert(
        task_id="task_insert_raw_matchs",
        xcom_source="task_fetch_puuid_info",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_raw_puuid_info",
        schema="lol_raw_datas",
        if_table_exists="replace",
        add_technical_columns=True,
    )

    # Transformation des données brutes en données factuelles
    task_raw_to_fact_matchs = load.Warehouse.raw_to_fact(
        task_id="task_raw_to_fact",
        outlets=[Asset('warehouse://lol_fact_datas/lol_fact_puuid')],
        source_table="lol_raw_datas.lol_raw_puuid_info",
        target_table="lol_fact_datas.lol_fact_puuid",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        has_not_matched=False,
        has_matched=True,
        join_keys=["puuid"],
        match_columns={
            "game_name": "game_name",
            "tag_line": "tag_line",
            "queue_type": "queue_type",
            "tier": "tier",
            "rank": "rank",
            "date_processed": "CURRENT_TIMESTAMP",
        },
    )
    
    # Définition de l'ordre d'exécution des tâches
    chain(
        task_get_puuid,
        task_fetch_puuid_info,
        task_insert_raw_matchs,
        task_raw_to_fact_matchs,
    )