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
            raise ValueError("❌ La variable 'LOL_Riot-Token' n'est pas définie dans Airflow.")

        if not Api_riotgames.HTTP_HOST:
            raise ValueError("❌ Le host de l'API Riot Games n'est pas défini dans Airflow.")

        if not Api_riotgames.HTTP_HEADERS:
            raise ValueError("❌ Les headers de l'API Riot Games ne sont pas définis dans Airflow.")

        if 'X-Riot-Token' not in Api_riotgames.HTTP_HEADERS:
            raise ValueError("❌ Le header 'X-Riot-Token' de l'API Riot Games n'est pas défini dans Airflow.")

        if not Api_riotgames.HTTP_HEADERS['X-Riot-Token']:
            raise ValueError("❌ Le header 'X-Riot-Token' de l'API Riot Games est vide dans Airflow.")

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
            raise ValueError("❌ Le PUUID n'a pas été trouvé dans la source XCom fournie.")

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

