import sys
import os
import pandas as pd
import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.extraction as extraction
import app.tasks.load as load
import app.tasks.transformation as transformation
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_referentiel"
DESCRIPTION = "Ce DAG extrait, transforme et charge les données des champions League of Legends depuis l'API DDragon vers l'entrepôt de données."
OBJECTIF = "Extraire, transformer et charger les données des champions League of Legends dans l'entrepôt de données."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=10), # Temps entre chaque tentative
}

class Custom():

    @customTask
    @staticmethod
    def list_champions(
        xcom_source : str,
        **context
    ):
        """ Crée une liste des champions avec les informations demandées.

        Args:
            xcom_source (str): Source XCom contenant les données brutes des champions.

        Returns:
            pd.DataFrame: DataFrame contenant la liste des champions avec les informations demandées.
        """

        data_xcom = manager.Xcom.get(xcom_source=xcom_source, to_df=False, **context)
        champions_data = data_xcom["data"]

        # Créer une liste des champions avec les informations demandées
        champions_list = []
        for _, champion_info in champions_data.items():
            # Convertir les tags en string JSON pour éviter le problème numpy.ndarray
            tags = champion_info.get('tags', [])
            tags_str = ','.join(tags) if tags else ''

            champion = {
                'name': champion_info.get('name', ''),
                'id': champion_info.get('id', ''),
                'key': champion_info.get('key', ''),
                'title': champion_info.get('title', ''),
                'description': champion_info.get('blurb', ''),  # La description est dans 'blurb'
                'tags': tags_str
            }
            champions_list.append(champion)

        # Convertir la liste en DataFrame pandas
        champions_df = pd.DataFrame(champions_list)

        return manager.Xcom.put(
            input=champions_df,
            xcom_strategy='file',
            **context
        )

    @staticmethod
    def get_ddragon_version() -> str:
        """ Récupère la dernière version de DDragon depuis l'API.

        Returns:
            str: Dernière version de DDragon
        """
        http_details = manager.Connectors.http(conn_id="API_LOL_ddragon")
        url = f"{http_details['host']}/api/versions.json"

        response = requests.get(url, headers=http_details.get('headers'))
        response.raise_for_status()
        data = response.json()

        return data[0]

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

    Custom.get_ddragon_version()

    task_get_champions_list = extraction.ApiProviders.get(
        conn_id="API_LOL_ddragon",
        endpoint=f"/cdn/{Custom.get_ddragon_version()}/data/fr_FR/champion.json",
        to_dataframe=False,
        xcom_strategy='file',
        file_format='json',
        task_id="task_get_champions_list",
    )

    task_list_champions = Custom.list_champions(
        task_id="task_list_champions",
        xcom_source="task_get_champions_list",
    )

    task_add_tech_info = transformation.AddColumns.tech_info(
        task_id="task_add_tech_info",
        xcom_source="task_list_champions",
    )

    task_add_tech_photo = transformation.AddColumns.tech_photo(
        task_id="task_add_tech_date",
        xcom_source="task_add_tech_info",
    )

    task_insert_champions_list = load.Warehouse.insert(
        task_id="task_insert_champions_list",
        xcom_source="task_add_tech_date",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="ref_champions",
        schema="lol_referentiel",
        if_table_exists="replace",
    )

    chain(
        task_get_champions_list,
        task_list_champions,
        task_add_tech_info,
        task_add_tech_photo,
        task_insert_champions_list,
    )
