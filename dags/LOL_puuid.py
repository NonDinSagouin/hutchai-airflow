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
import app.tasks.autres as autres
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_puuid"
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
    dagrun_timeout=timedelta(hours=2),  # Temps maximum d'exécution d'un DAG
    description=DESCRIPTION,
    doc_md=f"""
        ## 🔹 Description
        {DESCRIPTION}

        ## 🔹 Objectif
        {OBJECTIF}
    """,
) as dag:

    fetch_entries = extraction.Api_riotgames.fetch_entries_by_league(
        task_id='fetch_entries',
        division='III',
        tier='SILVER',
        queue='RANKED_SOLO_5x5',
        min_pages=1,
        max_pages=2000,
        rate_limit_per_batch=100,
        sleep_time_between_batches=125,
    )

    with TaskGroup("transformation_tasks") as transformation_tasks:


        task_rename_columns = transformation.Clean.rename_columns(
            task_id="task_rename_columns",
            xcom_source="fetch_entries",
            columns_mapping={
                "queueType": "queue_type",
            },
        )

        task_drop_duplicates = transformation.Clean.drop_duplicates(
            task_id="task_drop_duplicates",
            xcom_source="transformation_tasks.task_rename_columns",
            subset_columns=["puuid", "queue_type"],
        )

        task_insert_lol_raw = load.Warehouse.insert(
            task_id="task_insert_lol_raw",
            xcom_source="transformation_tasks.task_drop_duplicates",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_puuid",
            schema="lol_datas",
            if_table_exists="replace",
            add_technical_columns=True,
        )

        task_raw_to_fact = load.Warehouse.raw_to_fact(
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
                "tier": "tier",
                'rank': "rank",
                'wins': "wins",
                'losses': "losses",
                'queue_type': "queue_type",
            },
        )

        chain(task_rename_columns, task_drop_duplicates, task_insert_lol_raw, task_raw_to_fact)

    chain(
        fetch_entries,
        transformation_tasks,
    )
