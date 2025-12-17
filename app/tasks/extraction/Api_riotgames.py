import logging
import requests
import pandas as pd


from typing import Any
from datetime import datetime, timedelta
from airflow.sdk import Variable
from airflow.exceptions import AirflowFailException

import app.manager as manager
import app.helper as helper
from app.tasks.decorateurs import customTask

class Api_riotgames():

    @staticmethod
    def __awake():

        Api_riotgames.LOL_RIOT_TOKEN = Variable.get("LOL_Riot-Token")
        Api_riotgames.HTTP_DETAILS = manager.Connectors.http("API_LOL_riot")
        Api_riotgames.HTTP_HOST = Api_riotgames.HTTP_DETAILS.get('host', '')

        Api_riotgames.HTTP_HEADERS = Api_riotgames.HTTP_DETAILS.get('headers', {})
        Api_riotgames.HTTP_HEADERS['X-Riot-Token'] = Api_riotgames.LOL_RIOT_TOKEN

        if not Api_riotgames.LOL_RIOT_TOKEN:
            raise AirflowFailException("❌ La variable 'LOL_Riot-Token' n'est pas définie dans Airflow.")

        if not Api_riotgames.HTTP_HOST:
            raise AirflowFailException("❌ Le host de l'API Riot Games n'est pas défini dans Airflow.")

        if not Api_riotgames.HTTP_HEADERS:
            raise AirflowFailException("❌ Les headers de l'API Riot Games ne sont pas définis dans Airflow.")

        if 'X-Riot-Token' not in Api_riotgames.HTTP_HEADERS:
            raise AirflowFailException("❌ Le header 'X-Riot-Token' de l'API Riot Games n'est pas défini dans Airflow.")

        if not Api_riotgames.HTTP_HEADERS['X-Riot-Token']:
            raise AirflowFailException("❌ Le header 'X-Riot-Token' de l'API Riot Games est vide dans Airflow.")

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

        # Calculer les timestamps des 7 derniers jours
        # start_timestamp = int((datetime.now() - timedelta(days=7)).timestamp())
        # end_timestamp = int(datetime.now().timestamp())
        # &startTime={start_timestamp}&endTime={end_timestamp}

        endpoint = f"/lol/match/v5/matches/by-puuid/{lol_puuid}/ids?start={start}&count={count}"
        url = f"{Api_riotgames.HTTP_HOST}{endpoint}"

        return helper.call_api(
            url=url,
            headers=Api_riotgames.HTTP_HEADERS,
        )

    @staticmethod
    def __treatment_match_detail(
        match_id: str,
    ) -> dict:
        
        endpoint = f"/lol/match/v5/matches/{match_id}"
        url = f"{Api_riotgames.HTTP_HOST}{endpoint}"
        
        match_details = helper.call_api(
            url=url,
            headers=Api_riotgames.HTTP_HEADERS,
        )
        logging.info(f"✅ Récupération des détails du match réussie pour le match ID: {match_id}")

        metadata = match_details.get('metadata')
        puuid_participants = metadata.get('participants')
        logging.info(f"✅ PUUID des participants récupérés: {len(puuid_participants)} participants")

        info = match_details.get('info')
        match_data = {
            "match_id": metadata.get('matchId'),
            "game_creation": info.get('gameCreation'), # Timestamp de création
            "game_duration": info.get('gameDuration'), # Durée du match
            "game_mode": info.get('gameMode'), # Mode de jeu (CLASSIC, ARAM, etc.)
            "game_version": info.get('gameVersion'), # Version du jeu
            "game_in_progress": info.get('gameEndTimestamp') is None, # Indique si le match est en cours
        }
        logging.info(f"✅ Données générales du match récupérées: {match_data}")

        info_participants = info.get('participants')
        stats_participants = []

        for participant in info_participants:
            stats = {
                "match_id": metadata.get('matchId'),
                "game_creation": datetime.fromtimestamp(match_data.get('game_creation') / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "game_version": info.get('gameVersion'),
                "game_mode": info.get('gameMode'),
                "champion_id": participant.get('championId'),
                "champion_name": participant.get('championName'),

                "kills": participant.get('kills'),
                "deaths": participant.get('deaths'),
                "assists": participant.get('assists'),
                "kda": round((participant.get('kills') + participant.get('assists')) / max(1, participant.get('deaths')), 2),
                "double_kills": participant.get('doubleKills'),
                "triple_kills": participant.get('tripleKills'),
                "quadra_kills": participant.get('quadraKills'),
                "penta_kills": participant.get('pentaKills'),
                "largest_killing_spree": participant.get('largestKillingSpree'),

                "total_damage_dealt": participant.get('totalDamageDealt'),
                "total_damage_dealt_to_champions": participant.get('totalDamageDealtToChampions'),
                "physical_damage_dealt_to_champions": participant.get('physicalDamageDealtToChampions'),
                "magic_damage_dealt_to_champions": participant.get('magicDamageDealtToChampions'),
                "true_damage_dealt_to_champions": participant.get('trueDamageDealtToChampions'),
                "largest_critical_strike": participant.get('largestCriticalStrike'),

                "total_damage_taken": participant.get('totalDamageTaken'),
                "physical_damage_taken": participant.get('physicalDamageTaken'),
                "magic_damage_taken": participant.get('magicDamageTaken'),
                "true_damage_taken": participant.get('trueDamageTaken'),
                "total_heal": participant.get('totalHeal'),
                "total_heals_on_teammates": participant.get('totalHealsOnTeammates'),

                "total_minions_killed": participant.get('totalMinionsKilled'),
                "gold_earned": participant.get('goldEarned'),

                "champ_level": participant.get('champLevel'),
                "champ_experience": participant.get('champExperience'),
            }

            stats_participants.append(stats)
            logging.debug(f"✅ Statistiques de {stats.get('champion_name')} récupérées")

        logging.info(f"✅ Statistiques des participants récupérées: {len(stats_participants)} participants")

        return {
            "puuid_participants": puuid_participants,
            "match_data" : match_data,
            "stats_participants": stats_participants,
        }
    
    @customTask
    @staticmethod
    def fetch_matchs_by_puuid(
        xcom_source: str,
        **context
    ) -> Any:
        """ Permet de récupérer les identifiants des matchs d'un joueur via l'API Riot Games.

        Args:
            xcom_source (str): Source XCom contenant le PUUID du joueur League of Legends.
            **context: Contexte d'exécution Airflow.

        Returns:
            Any: DataFrame contenant les identifiants des matchs récupérés.
        """

        lol_puuid = manager.Xcom.get(
            xcom_source=xcom_source,
            **context
        )

        if lol_puuid.empty or 'puuid' not in lol_puuid.columns:
            raise AirflowFailException("❌ Le PUUID n'a pas été trouvé dans la source XCom fournie.")

        lol_puuid = lol_puuid['puuid'].iloc[0]

        Api_riotgames.__awake()

        matchs = []
        start = 0
        count = 100
        nb_iterations = 0

        while True:

            if nb_iterations > 20:
                raise AirflowFailException("❌ Nombre maximum d'itérations atteint lors de la récupération des matchs.")

            matches = Api_riotgames.__get_matches(lol_puuid, start, count)
            nb_iterations += 1

            if not matches: break

            matchs.extend(matches)
            start += count

            if start >= 1000: break

        df_matches = pd.DataFrame(matchs)
        df_matches.columns = ['match_id']
        logging.info(f"✅ Conversion des données de match en DataFrame réussie. Nombre de lignes: {len(df_matches)}")

        return manager.Xcom.put(
            input=df_matches,
            **context
        )

    @customTask
    @staticmethod
    def fetch_match_details(
        xcom_source: str,
        **context
    ) -> Any:
        """ Permet de récupérer les détails d'un match via l'API Riot Games.

        Args:
            xcom_source (str): Source XCom contenant l'identifiant du match.
            **context: Contexte d'exécution Airflow.

        Returns:
            Any: DataFrame contenant les détails du match récupéré.
        """

        matchs_id = manager.Xcom.get(
            xcom_source=xcom_source,
            **context
        )

        if matchs_id.empty or 'match_id' not in matchs_id.columns:
            raise AirflowFailException("❌ L'identifiant des matchs n'ont pas été trouvés dans la source XCom fournie.")

        Api_riotgames.__awake()

        all_match_details = {
            "match_data": [],
            "puuid_participants": [],
            "stats_participants": [],
        }

        for match_id in matchs_id['match_id']:

            match_details = Api_riotgames.__treatment_match_detail(
                match_id=match_id,
            )

            if match_details:
                all_match_details["match_data"].append(match_details["match_data"])
                all_match_details["puuid_participants"].extend(match_details["puuid_participants"])
                all_match_details["stats_participants"].extend(match_details["stats_participants"])

        all_match_details["match_data"] = pd.DataFrame(all_match_details["match_data"])
        all_match_details["puuid_participants"] = pd.DataFrame(all_match_details["puuid_participants"], columns=['puuid'])
        all_match_details["stats_participants"] = pd.DataFrame(all_match_details["stats_participants"])

        return manager.Xcom.put(
            input=all_match_details,
            **context
        )