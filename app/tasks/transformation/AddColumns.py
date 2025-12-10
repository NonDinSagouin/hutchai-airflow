import pandas as pd
import logging
import pendulum

from sqlalchemy.engine import Engine
from sqlalchemy import text
from airflow.exceptions import AirflowFailException

import app.helper as helper
import app.manager as manager
from app.tasks.decorateurs import customTask

class AddColumns():

    @staticmethod
    def __get_execution_date(**kwargs) -> pendulum.DateTime:
        """Méthode privée pour récupérer le DataFrame et la date d'exécution.

        Returns:
            pendulum.DateTime: Date d'exécution
        """
        PARIS_TZ = "Europe/Paris"

        # Récupération de la date d'exécution selon la version d'Airflow
        if 'logical_date' in kwargs:
            execution_date = kwargs['logical_date'].in_timezone(pendulum.timezone(PARIS_TZ))
        elif 'execution_date' in kwargs:
            execution_date = kwargs['execution_date'].in_timezone(pendulum.timezone(PARIS_TZ))
        else:
            # Fallback sur ds (date string) si ni logical_date ni execution_date ne sont disponibles
            execution_date = pendulum.parse(kwargs['ds']).in_timezone(pendulum.timezone(PARIS_TZ))

        return execution_date

    @customTask
    @staticmethod
    def tech_info(
        xcom_source : str,
        **kwargs
    ) -> pd.DataFrame:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
        """

        df_xcom = manager.Xcom.get(xcom_source, **kwargs)
        execution_date = AddColumns.__get_execution_date(**kwargs)

        df_xcom['tech_dag_id'] = kwargs['dag'].dag_id
        df_xcom['tech_execution_date'] = execution_date.strftime("%Y-%m-%d %H:%M:%S")

        return df_xcom

    @customTask
    @staticmethod
    def tech_photo(
        xcom_source : str,
        date_column: str = "tech_date_photo",
        year_column: str = "tech_annee_photo",
        week_column: str = "tech_semaine_photo",
        **kwargs
    ) -> pd.DataFrame:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
            date_column (str, optional): Nom de la colonne pour la date d'exécution. Defaults to "tech_date_photo".
            year_column (str, optional): Nom de la colonne pour l'année d'exécution. Defaults to "tech_annee_photo".
            week_column (str, optional): Nom de la colonne pour la semaine d'exécution. Defaults to "tech_semaine_photo".
        """

        df_xcom = manager.Xcom.get(xcom_source, **kwargs)
        execution_date = AddColumns.__get_execution_date(**kwargs)

        iso_year_today, iso_week_today, _ = execution_date.isocalendar()

        df_xcom[date_column] = execution_date.strftime("%Y-%m-%d")
        df_xcom[year_column] = iso_year_today
        df_xcom[week_column] = iso_week_today

        return df_xcom