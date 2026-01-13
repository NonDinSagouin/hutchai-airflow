import sys
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, Asset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.transformation as transformation
import app.tasks.api as api
import app.tasks.databases as databases
import app.manager as manager
import app.library as library

DAG_ID = "LOL_enrich_fact_matchs"
DESCRIPTION = "Récupération des informations de matchs des joueurs League of Legends via l'API Riot Games et stockage dans l'entrepôt de données."
OBJECTIF = " Ce DAG vise à extraire les PUUIDs des joueurs League of Legends depuis une table factuelle, " \
"interroger l'API Riot Games pour obtenir les identifiants des matchs associés à chaque PUUID," \
"stocker ces identifiants dans une table de données brutes, puis transformer ces données en une table factuelle dans l'entrepôt de données."
REMARQUE = "Assurez-vous que les clés API Riot Games sont correctement configurées dans le gestionnaire de connexions avant d'exécuter ce DAG." \
" Si il n'y a pas de nouvelles données à traiter, le DAG skipera les étapes inutiles."
SCHEDULE = "0 09 * * *"  # 09h00 tous les jours
START_DATE = datetime(2025, 1, 1)
TAGS = [library.TagsLibrary.LEAGUE_OF_LEGENDS, library.TagsLibrary.RIOT_GAMES, library.TagsLibrary.WAREHOUSE, library.TagsLibrary.DATA_ROWS, library.TagsLibrary.DATA_FACT]

LIMIT_PUUID = 10  # Nombre de PUUIDs à traiter par exécution
QUEUE_ARAM = 450  # Nombre de matchs à récupérer par PUUID

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 1, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(minutes=2, seconds=10), # Temps entre chaque tentative
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
    get_puuid = databases.PostgresWarehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid_to_process",
        schema="lol_fact_datas",
        task_id="get_puuid",
        schema_select={"puuid"},
        schema_order="date_processed ASC",
        limit=LIMIT_PUUID,
        skip_empty=True,
    )

    # Récupération des identifiants de matchs via l'API Riot Games
    fetch_matchs_by_puuid = api.Riotgames.fetch_matchs_by_puuid(
        task_id = "fetch_matchs_by_puuid",
        xcom_source="get_puuid",
        queue=QUEUE_ARAM,
    )

    # Suppression des doublons éventuels
    drop_duplicates_stats = transformation.Clean.drop_duplicates(
        task_id="drop_duplicates_stats",
        xcom_source="fetch_matchs_by_puuid",
        subset_columns=["match_id"],
    )

    # Insertion des données brutes dans la table d'entrepôt
    insert_raw_matchs = databases.PostgresWarehouse.insert(
        task_id="insert_raw_matchs",
        xcom_source="drop_duplicates_stats",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_raw_match_datas_ids",
        schema="lol_raw_datas",
        if_table_exists="replace",
        add_technical_columns=True,
    )

    # Transformation des données brutes en données factuelles
    raw_to_fact_matchs = databases.PostgresWarehouse.raw_to_fact(
        task_id="raw_to_fact",
        outlets=[Asset('warehouse://lol_fact_datas/lol_fact_match')],
        source_table="lol_raw_datas.lol_raw_match_datas_ids",
        target_table="lol_fact_datas.lol_fact_match",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        has_not_matched=True,
        has_matched=False,
        join_keys=["match_id"],
        non_match_columns={
            "match_id": "match_id",
        },
    )

    # Mise à jour de la date de traitement des PUUIDs
    update_puuid_status = databases.PostgresWarehouse.update(
        task_id="update_puuid_status",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid_to_process",
        schema="lol_fact_datas",
        set_values={
            "date_processed": "CURRENT_TIMESTAMP",
        },
        where_conditions={
            "puuid": "(select puuid from lol_raw_datas.lol_raw_match_datas_ids)",
        },
    )

    # Définition de l'ordre d'exécution des tâches
    chain(
        get_puuid,
        fetch_matchs_by_puuid,
        drop_duplicates_stats,
        insert_raw_matchs,
        raw_to_fact_matchs,
        update_puuid_status,
    )
