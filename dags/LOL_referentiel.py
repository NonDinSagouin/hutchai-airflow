import sys
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.data_extraction as data_extraction
import app.tasks.warehouse as warehouse

from app import manager
from app.tasks.decorateurs import customTask


DAG_ID = "LOL_referentiel"
DESCRIPTION = ""
OBJECTIF = ""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=10), # Temps entre chaque tentative
}

class CustomTask():

    @customTask
    @staticmethod
    def list_champions(
        xcom_source : str,
        **context
    ):
        ti = context['ti']
        data = ti.xcom_pull(task_ids=xcom_source)
        champions = data['data'].keys()

        print("Liste des champions :", champions)
        return list(champions)

# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=datetime(2025, 1, 1), # Date de début du DAG
    #schedule="00 1 * * 1-7",  # Fréquence d'exécution (CRON ou timedelta)
    tags=["exemple",], # Liste de tags pour catégoriser le DAG dans l'UI
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
    
    task_get_champions_list = data_extraction.Providers.api_get(
        task_id="task_get_champions_list",
        conn_id="API_LOL_ddragon",
        endpoint="/cdn/15.23.1/data/fr_FR/champion.json",
    )

    task_list_champions = CustomTask.list_champions(
        task_id="task_list_champions",
        xcom_source="task_get_champions_list",
    )

    task_insert_champions_list = warehouse.Insert.basic(
        task_id="task_insert_champions_list",
        xcom_source="task_list_champions",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="ref_champions",
        schema="lol_referentiel",
        if_table_exists="replace",
    )

    chain(
        task_get_champions_list,
        task_list_champions,
        task_insert_champions_list,
    )
