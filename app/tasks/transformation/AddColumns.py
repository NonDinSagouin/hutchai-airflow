import pandas as pd
import logging
import pendulum

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

        logging.debug(f"📅 Date d'exécution récupérée : {execution_date}")

        return execution_date

    @customTask
    @staticmethod
    def tech_info(
        xcom_source : str,
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **kwargs
    ) -> pd.DataFrame | dict | str:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
            xcom_strategy (str, optional): Stratégie de stockage XCom ('auto', 'direct', 'file'). Defaults to 'auto'.
            file_format (str, optional): Format de fichier pour le stockage XCom ('json' ou 'parquet'). Defaults to 'parquet'.

        Returns:
            pd.DataFrame | dict | str: Donnée avec les colonnes techniques ajoutées, sous forme de DataFrame, dictionnaire ou chaîne de caractères selon les paramètres.

        Examples:
            >>> df_with_tech_info = AddColumns.tech_info(
            ...     xcom_source="extract_data_task",
            ...     xcom_strategy="auto",
            ...     file_format="parquet",
            ...     **kwargs
            ... )
            >>> print(df_with_tech_info.head())
                column1  column2        tech_dag_id     tech_execution_date
            0        ...     ...        example_dag     2024-07-22 01:00:00
        """

        df_xcom = manager.Xcom.get(xcom_source, **kwargs)
        execution_date = AddColumns.__get_execution_date(**kwargs)

        df_xcom['tech_dag_id'] = kwargs['dag'].dag_id
        df_xcom['tech_execution_date'] = execution_date.strftime("%Y-%m-%d %H:%M:%S")

        logging.info("✅ Colonnes techniques 'tech_dag_id' et 'tech_execution_date' ajoutées au DataFrame.")

        return manager.Xcom.put(
            input=df_xcom,
            xcom_strategy=xcom_strategy,
            file_format=file_format,
            **kwargs
        )

    @customTask
    @staticmethod
    def tech_photo(
        xcom_source : str,
        date_column: str = "tech_date_photo",
        year_column: str = "tech_annee_photo",
        week_column: str = "tech_semaine_photo",
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **kwargs
    ) -> pd.DataFrame:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
            date_column (str, optional): Nom de la colonne pour la date d'exécution. Defaults to "tech_date_photo".
            year_column (str, optional): Nom de la colonne pour l'année d'exécution. Defaults to "tech_annee_photo".
            week_column (str, optional): Nom de la colonne pour la semaine d'exécution. Defaults to "tech_semaine_photo".
            xcom_strategy (str, optional): Stratégie de stockage XCom ('auto', 'direct', 'file'). Defaults to 'auto'.
            file_format (str, optional): Format de fichier pour le stockage XCom ('json' ou 'parquet'). Defaults to 'parquet'.

        Returns:
            pd.DataFrame: DataFrame avec les colonnes techniques ajoutées
        """

        df_xcom = manager.Xcom.get(xcom_source, **kwargs)
        execution_date = AddColumns.__get_execution_date(**kwargs)

        iso_year_today, iso_week_today, _ = execution_date.isocalendar()

        df_xcom[date_column] = execution_date.strftime("%Y-%m-%d")
        df_xcom[year_column] = iso_year_today
        df_xcom[week_column] = iso_week_today

        logging.info(f"✅ Colonnes techniques '{date_column}', '{year_column}' et '{week_column}' ajoutées au DataFrame.")

        return manager.Xcom.put(
            input=df_xcom,
            xcom_strategy=xcom_strategy,
            file_format=file_format,
            **kwargs
        )