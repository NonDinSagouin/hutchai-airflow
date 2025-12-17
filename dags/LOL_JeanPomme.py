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
import app.helper as helper
import app.manager as manager

from app.tasks.decorateurs import customTask

DAG_ID = "LOL_JeanPomme"
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

    LOL_RIOT_TOKEN: str
    HTTP_DETAILS: dict
    HTTP_HOST: str
    HTTP_HEADERS: dict

    @customTask
    @staticmethod
    def fetch_matchs(
        lol_puuid: str,
        **context
    ) -> None:
        """ Tâche personnalisée pour récupérer des données de match depuis l'API Riot Games.
        Returns:
            None
        """

        if lol_puuid is None or lol_puuid == "":
            raise ValueError("❌ La variable 'LOL_puuid' n'est pas définie dans Airflow.")

        Custom.__init__()

        all_matches = []
        start = 0
        count = 100

        while True:

            matches = Custom.__get_matches(lol_puuid, start, count)
            if not matches: break

            all_matches.extend(matches)
            start += count
            if start >= 1000: break

        logging.info(f"✅ Récupération des données de match réussie. Nombre de matchs récupérés: {len(matches)}")

        return manager.Xcom.put(
            input=all_matches,
            **context
        )

    @customTask
    @staticmethod
    def fetch_match_details(
        xcom_match_id: str,
        **context
    ) -> dict:
        """ Tâche personnalisée pour récupérer les détails d'un match spécifique depuis l'API Riot Games.

        Args:
            match_id (str): L'identifiant du match à récupérer.

        Returns:
            dict: Les détails du match.
        """

        Custom.__init__()

        matchs = manager.Xcom.get(
            xcom_source=xcom_match_id,
            **context
        )
        match = matchs[0]

        match_details = Custom.__get_match_details(match)
        logging.info(f"✅ Récupération des détails du match réussie pour le match ID: {match}")

        metadata = match_details.get('metadata')
        puuid_participants = metadata.get('participants')
        logging.info(f"✅ PUUID des participants récupérés: {len(puuid_participants)} participants")

        info = match_details.get('info')
        match_data = {
            "matchId": metadata.get('matchId'),
            "gameCreation": info.get('gameCreation'), # Timestamp de création
            "gameDuration": info.get('gameDuration'), # Durée du match
            "gameMode": info.get('gameMode'), # Mode de jeu (CLASSIC, ARAM, etc.)
            "gameVersion": info.get('gameVersion'), # Version du jeu
        }
        logging.info(f"✅ Données générales du match récupérées: {match_data}")

        info_participants = info.get('participants')
        stats_participants = []

        for participant in info_participants:
            stats = {
                "matchId": metadata.get('matchId'),
                "gameCreation": datetime.fromtimestamp(match_data.get('gameCreation') / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "gameVersion": info.get('gameVersion'),
                "gameMode": info.get('gameMode'),

                "championId": participant.get('championId'),
                "championName": participant.get('championName'),

                "kills": participant.get('kills'),
                "deaths": participant.get('deaths'),
                "assists": participant.get('assists'),
                "kda": round((participant.get('kills') + participant.get('assists')) / max(1, participant.get('deaths')), 2),
                "doubleKills": participant.get('doubleKills'),
                "tripleKills": participant.get('tripleKills'),
                "quadraKills": participant.get('quadraKills'),
                "pentaKills": participant.get('pentaKills'),
                "largestKillingSpree": participant.get('largestKillingSpree'),

                "totalDamageDealt": participant.get('totalDamageDealt'),
                "totalDamageDealtToChampions": participant.get('totalDamageDealtToChampions'),
                "physicalDamageDealtToChampions": participant.get('physicalDamageDealtToChampions'),
                "magicDamageDealtToChampions": participant.get('magicDamageDealtToChampions'),
                "trueDamageDealtToChampions": participant.get('trueDamageDealtToChampions'),
                "largestCriticalStrike": participant.get('largestCriticalStrike'),

                "totalDamageTaken": participant.get('totalDamageTaken'),
                "physicalDamageTaken": participant.get('physicalDamageTaken'),
                "magicDamageTaken": participant.get('magicDamageTaken'),
                "trueDamageTaken": participant.get('trueDamageTaken'),
                "totalHeal": participant.get('totalHeal'),
                "totalHealsOnTeammates": participant.get('totalHealsOnTeammates'),

                "totalMinionsKilled": participant.get('totalMinionsKilled'),
                "goldEarned": participant.get('goldEarned'),

                "champLevel": participant.get('champLevel'),
                "champExperience": participant.get('champExperience'),
            }

            stats_participants.append(stats)
            logging.debug(f"✅ Statistiques de {stats.get('championName')} récupérées")

        logging.info(f"✅ Statistiques des participants récupérées: {len(stats_participants)} participants")

        match_statics = {
            "puuid_participants": puuid_participants,
            "match_data" : match_data,
            "stats_participants": stats_participants,
        }

        return manager.Xcom.put(
            input=match_statics,
            **context
        )

    @customTask
    @staticmethod
    def stats_participants_transform(
        xcmm_source: dict,
        **context
    ) -> pd.DataFrame:
        """ Tâche personnalisée pour extraire les statistiques des participants d'un match.

        Args:
            match_details (dict): Les détails du match.

        Returns:
            list: Une liste des statistiques des participants.
        """

        match_details = manager.Xcom.get(
            xcom_source=xcmm_source,
            **context
        )

        stats_participants = match_details.get('stats_participants')
        df_stats_participants = pd.DataFrame(stats_participants)

        logging.info(f"✅ Extraction des statistiques des participants réussie: {len(df_stats_participants)} participants")

        return df_stats_participants

    @staticmethod
    def __get_matches(
        lol_puuid: str,
        start: int,
        count: int,
    ) -> list:
        """ Permet de récupérer les identifiants des matchs d'un joueur via l'API Riot Games.

        Args:
            lol_puuid (str): Le PUUID du joueur League of Legends.
            start (int): L'index de départ pour la pagination.
            count (int): Le nombre de matchs à récupérer.

        Returns:
            list: Une liste d'identifiants de matchs.
        """

        endpoint = f"/lol/match/v5/matches/by-puuid/{lol_puuid}/ids?start={start}&count={count}"
        url = f"{Custom.HTTP_HOST}{endpoint}"

        return helper.call_api(
            url=url,
            headers=Custom.HTTP_HEADERS,
        )

    @staticmethod
    def __get_match_details(
        match_id: str,
    ) -> list:
        """ Permet de récupérer les identifiants des matchs d'un joueur via l'API Riot Games.

        Args:
            lol_puuid (str): Le PUUID du joueur League of Legends.
            start (int): L'index de départ pour la pagination.
            count (int): Le nombre de matchs à récupérer.

        Returns:
            list: Une liste d'identifiants de matchs.
        """

        endpoint = f"/lol/match/v5/matches/{match_id}"
        url = f"{Custom.HTTP_HOST}{endpoint}"

        return helper.call_api(
            url=url,
            headers=Custom.HTTP_HEADERS,
        )

    @staticmethod
    def __init__():

        Custom.LOL_RIOT_TOKEN = Variable.get("LOL_Riot-Token")
        Custom.HTTP_DETAILS = manager.Connectors.http("API_LOL_riot")
        Custom.HTTP_HOST = Custom.HTTP_DETAILS.get('host', '')

        Custom.HTTP_HEADERS = Custom.HTTP_DETAILS.get('headers', {})
        Custom.HTTP_HEADERS['X-Riot-Token'] = Custom.LOL_RIOT_TOKEN

        if not Custom.LOL_RIOT_TOKEN:
            raise ValueError("❌ La variable 'LOL_Riot-Token' n'est pas définie dans Airflow.")

        if not Custom.HTTP_HOST:
            raise ValueError("❌ Le host de l'API Riot Games n'est pas défini dans Airflow.")

        if not Custom.HTTP_HEADERS:
            raise ValueError("❌ Les headers de l'API Riot Games ne sont pas définis dans Airflow.")

        if 'X-Riot-Token' not in Custom.HTTP_HEADERS:
            raise ValueError("❌ Le header 'X-Riot-Token' de l'API Riot Games n'est pas défini dans Airflow.")

        if not Custom.HTTP_HEADERS['X-Riot-Token']:
            raise ValueError("❌ Le header 'X-Riot-Token' de l'API Riot Games est vide dans Airflow.")

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

    task_fetch_matchs = Custom.fetch_matchs(
        task_id = "task_fetch_matchs",
        lol_puuid="oQ-amckTjjNgCnqnLSdin0MheBwwLqtLsOZn291ntYI91ZuTUQ3kUJk6w2V794xrzXbbKzC5EmklSA",
    )
    task_fetch_match_details = Custom.fetch_match_details(
        task_id = "task_fetch_match_details",
        xcom_match_id="task_fetch_matchs",
    )

    task_stats_participants_transform = Custom.stats_participants_transform(
        task_id = "task_stats_participants_transform",
        xcmm_source="task_fetch_match_details",
    )

    task_insert_stats_participants = load.Warehouse.insert(
        task_id="task_insert_stats_participants",
        xcom_source="task_stats_participants_transform",
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        table_name="lol_stats",
        schema="lol_datas",
        if_table_exists="replace",
    )

    chain(
        task_fetch_matchs,
        task_fetch_match_details,
        task_stats_participants_transform,
        task_insert_stats_participants,
    )
