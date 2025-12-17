import sys
import os
import logging
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, Variable, Asset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.extraction as extraction
import app.tasks.load as load
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_matchs"
DESCRIPTION = ""
OBJECTIF = ""
SCHEDULE = "*/2 * * * *", # Toutes les 2 minutes
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

    task_get_puuid = load.Warehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid",
        schema="lol_datas",
        task_id="task_get_puuid",
        shema_select={"puuid", "game_name", "tag_line"},
        shema_order="date_processed DESC",
        limit=1,
    )

    task_fetch_matchs_by_puuid = extraction.Api_riotgames.fetch_matchs_by_puuid(
        task_id = "task_fetch_matchs_by_puuid",
        xcom_source="task_get_puuid",
    )

    task_insert_raw_matchs = load.Warehouse.insert(
        task_id="task_insert_raw_matchs",
        xcom_source="task_fetch_matchs_by_puuid",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_raw_match_datas_ids",
        schema="lol_datas",
        if_table_exists="replace",
        add_technical_columns=True,
    )

    task_raw_to_fact_matchs = load.Warehouse.raw_to_fact(
        task_id="task_raw_to_fact",
        outlets=[Asset('warehouse://lol_datas/lol_fact_match_datas')],
        source_table="lol_datas.lol_raw_match_datas_ids",
        target_table="lol_datas.lol_fact_match_datas",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        has_not_matched=True,
        has_matched=False,
        join_keys=["match_id"],
        non_match_columns={
            "match_id": "match_id",
        },
    )

    task_update_puuid_status = load.Warehouse.update(
        task_id="task_update_puuid_status",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_fact_puuid",
        schema="lol_datas",
        set_values={
            "date_processed": "CURRENT_TIMESTAMP",
        },
        where_conditions={
            "puuid": "{{ ti.xcom_pull(task_ids='task_get_puuid')['puuid'].iloc[0] }}",
        },
    )

    chain(
        task_get_puuid,
        task_fetch_matchs_by_puuid,
        task_insert_raw_matchs,
        task_raw_to_fact_matchs,
        task_update_puuid_status,
    )
