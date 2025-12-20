import logging
import hashlib
import pandas as pd

from typing import Any
from datetime import datetime
from airflow.sdk import Variable
from airflow.exceptions import AirflowFailException

import app.manager as manager
import app.helper as helper
from app.tasks.decorateurs import customTask

class Api_riotgames():

    @staticmethod
    def __awake(http = 'classic') -> None:
        """ Permet d'initialiser les variables nécessaires pour appeler l'API Riot Games.
        Args:
            http (str): Type d'instance HTTP à utiliser ('classic' ou 'euw1').
        """

        if http == 'euw1':
            Api_riotgames.HTTP_DETAILS = manager.Connectors.http("API_LOL_riot_euw1")
            Api_riotgames.HTTP_HOST = Api_riotgames.HTTP_DETAILS.get('host', '')
            Api_riotgames.HTTP_HEADERS = Api_riotgames.HTTP_DETAILS.get('headers', {})
        else:
            Api_riotgames.HTTP_DETAILS = manager.Connectors.http("API_LOL_riot")
            Api_riotgames.HTTP_HOST = Api_riotgames.HTTP_DETAILS.get('host', '')
            Api_riotgames.HTTP_HEADERS = Api_riotgames.HTTP_DETAILS.get('headers', {})

        Api_riotgames.LOL_RIOT_TOKEN = Variable.get("LOL_Riot-Token")
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
        queue: int = 440,
    ) -> list:
        """ Permet de récupérer les identifiants des matchs d'un joueur via l'API Riot Games.

        Args:
            lol_puuid (str): Le PUUID du joueur League of Legends.
            start (int): L'index de départ pour la pagination.
            count (int): Le nombre de matchs à récupérer.
            queue (int): Le type de file d'attente (par défaut 440 pour Ranked Solo/Duo). https://static.developer.riotgames.com/docs/lol/queues.json

        Returns:
            list: Une liste d'identifiants de matchs.
        """

        # Calculer les timestamps des 7 derniers jours
        # start_timestamp = int((datetime.now() - timedelta(days=7)).timestamp())
        # end_timestamp = int(datetime.now().timestamp())
        # &startTime={start_timestamp}&endTime={end_timestamp}

        endpoint = f"/lol/match/v5/matches/by-puuid/{lol_puuid}/ids?start={start}&count={count}&queue={queue}"
        url = f"{Api_riotgames.HTTP_HOST}{endpoint}"

        return helper.call_api(
            url=url,
            headers=Api_riotgames.HTTP_HEADERS,
        )

    @staticmethod
    def __get_entries_by_league(
        division: str,
        tier: str,
        queue: str,
        page: int = 1,
    ) -> Any:
        """ Permet de récupérer les entrées d'une ligue via l'API Riot Games.

        Args:
            division (str): La division de la ligue (I, II, III, IV).
            tier (str): Le tier de la ligue (IRON, BRONZE, SILVER, GOLD, PLATINUM, DIAMOND, EMERALD, DIAMOND).
            queue (str): Le type de file d'attente (RANKED_SOLO_5x5, RANKED_FLEX_SR, RANKED_FLEX_TT).

        Returns:
            Any: Les données des entrées de la ligue.
        """

        endpoint = f"/lol/league/v4/entries/{queue}/{tier}/{division}?page={page}"
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

        info = match_details.get('info')
        metadata = match_details.get('metadata')
        match_data = {
            "match_id": metadata.get('matchId'),
            "game_creation": info.get('gameCreation'), # Timestamp de création
            "game_duration": info.get('gameDuration'), # Durée du match
            "game_mode": info.get('gameMode'), # Mode de jeu (CLASSIC, ARAM, etc.)
            "game_version": info.get('gameVersion'), # Version du jeu
            "game_in_progress": info.get('gameEndTimestamp') is None, # Indique si le match est en cours
            "is_processed": True,
        }
        logging.info(f"✅ Données générales du match récupérées: {match_data}")

        info_participants = info.get('participants')
        stats_participants = []

        for participant in info_participants:
            stats = {
                "id": hashlib.md5(f"{metadata.get('matchId')}_{participant.get('puuid')}".encode()).hexdigest(),
                "match_id": metadata.get('matchId'),
                "puuid": participant.get('puuid'),
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
                "neutral_minions_killed": participant.get('neutralMinionsKilled'),
                "gold_earned": participant.get('goldEarned'),

                "champ_level": participant.get('champLevel'),
                "champ_experience": participant.get('champExperience'),
            }

            stats_participants.append(stats)
            logging.debug(f"✅ Statistiques de {stats.get('champion_name')} récupérées")

        logging.info(f"✅ Statistiques des participants récupérées: {len(stats_participants)} participants")

        return {
            "match_data" : match_data,
            "stats_participants": stats_participants,
        }

    @customTask
    @staticmethod
    def fetch_matchs_by_puuid(
        xcom_source: str,
        queue: int = 440,
        **context
    ) -> Any:
        """ Permet de récupérer les identifiants des matchs d'un joueur via l'API Riot Games.

        Args:
            xcom_source (str): Source XCom contenant le PUUID du joueur League of Legends.
            queue (int): Le type de file d'attente (par défaut 440 pour Ranked Solo/Duo). https://static.developer.riotgames.com/docs/lol/queues.json
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

        logging.info(f"✅ Récupération des matchs pour le PUUID: {lol_puuid}")
        Api_riotgames.__awake()

        matchs = []
        start = 0
        count = 100
        nb_iterations = 0

        while True:

            if nb_iterations > 10:
                raise AirflowFailException("❌ Nombre maximum d'itérations atteint lors de la récupération des matchs.")

            matches = Api_riotgames.__get_matches(lol_puuid, start, count, queue)
            nb_iterations += 1

            if not matches: break

            matchs.extend(matches)
            start += count

            if start >= 1000: break
            if len(matches) < count: break

        if not matchs:
            raise AirflowFailException("❌ Aucun match n'a été récupéré pour le PUUID fourni.")

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
            "stats_participants": [],
        }

        for match_id in matchs_id['match_id']:

            match_details = Api_riotgames.__treatment_match_detail(
                match_id=match_id,
            )

            if match_details:
                all_match_details["match_data"].append(match_details["match_data"])
                all_match_details["stats_participants"].extend(match_details["stats_participants"])

        all_match_details["match_data"] = pd.DataFrame(all_match_details["match_data"])
        all_match_details["stats_participants"] = pd.DataFrame(all_match_details["stats_participants"])

        return manager.Xcom.put(
            input=all_match_details,
            **context
        )

    @customTask
    @staticmethod
    def fetch_entries_by_league(
        division: str,
        tier: str,
        queue: str,
        max_pages: int = 1,
        **context
    ):
        """ Permet de récupérer les entrées d'une ligue via l'API Riot Games.

        Args:
            division (str): La division de la ligue (I, II, III, IV).
            tier (str): Le tier de la ligue (IRON, BRONZE, SILVER, GOLD, PLATINUM, DIAMOND, EMERALD, DIAMOND).
            queue (str): Le type de file d'attente (RANKED_SOLO_5x5, RANKED_FLEX_SR, RANKED_FLEX_TT).
            max_pages (int): Le nombre maximum de pages à récupérer.
            **context: Contexte d'exécution Airflow.

        Returns:
            Any: DataFrame contenant les entrées de la ligue récupérées.

        Example:
            >>> fetch_entries_by_league(
                    division='IV',
                    tier='IRON',
                    queue='RANKED_SOLO_5x5',
                    max_pages=10,
                )
                Récupère les entrées de la ligue IRON IV en file d'attente RANKED_SOLO_5x5 sur 10 pages.
                
        Raises:
            AirflowFailException: Si les paramètres fournis sont invalides.
        """

        if division not in ['I', 'II', 'III', 'IV']:
            raise AirflowFailException(f"❌ Division '{division}' invalide. Les valeurs valides sont: I, II, III, IV.")

        if tier not in ['IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND', 'EMERALD', 'DIAMOND']:
            raise AirflowFailException(f"❌ Tier '{tier}' invalide. Les valeurs valides sont: IRON, BRONZE, SILVER, GOLD, PLATINUM, DIAMOND, EMERALD, DIAMOND.")

        if queue not in ['RANKED_SOLO_5x5', 'RANKED_FLEX_SR', 'RANKED_FLEX_TT']:
            raise AirflowFailException(f"❌ Queue '{queue}' invalide. Les valeurs valides sont: RANKED_SOLO_5x5, RANKED_FLEX_SR, RANKED_FLEX_TT.")

        Api_riotgames.__awake(http='euw1')

        all_entries = []

        for page in range(1, max_pages + 1):

            logging.info(f"✅ Page {page}/{max_pages} ...")

            entries = Api_riotgames.__get_entries_by_league(
                division=division,
                tier=tier,
                queue=queue,
                page=page,
            )

            if not entries:
                break

            all_entries.extend(entries)

        df_entries = pd.DataFrame(all_entries)
        logging.info(f"✅ Total des entrées récupérées: {len(df_entries)}")

        return manager.Xcom.put(
            input=df_entries,
            **context
        )
