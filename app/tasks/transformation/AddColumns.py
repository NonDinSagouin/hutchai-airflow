import pandas as pd
import logging
import pendulum

import app.manager as manager

from app.tasks.decorateurs import customTask

class AddColumns():

    # Fuseau horaire de Paris
    PARIS_TZ = "Europe/Paris"

    @staticmethod
    def __get_execution_date(**kwargs) -> pendulum.DateTime:
        """Méthode privée pour récupérer le DataFrame et la date d'exécution.

        Returns:
            pendulum.DateTime: Date d'exécution
        """
        if 'logical_date' in kwargs:
            execution_date = kwargs['logical_date'].in_timezone(AddColumns.PARIS_TZ)
        elif 'execution_date' in kwargs:
            execution_date = kwargs['execution_date'].in_timezone(AddColumns.PARIS_TZ)
        elif 'ds' in kwargs:
            execution_date = pendulum.parse(kwargs['ds']).in_timezone(AddColumns.PARIS_TZ)
        else:
            raise KeyError("❌ Aucune date d'exécution trouvée dans le contexte (logical_date, execution_date, ds)")

        logging.debug(f"📅 Date d'exécution récupérée : {execution_date}")
        return execution_date

    @customTask
    @staticmethod
    def tech_info(
        xcom_source : str,
        dag_id_column: str = "tech_dag_id",
        execution_date_column: str = "tech_execution_date",
        date_format: str = "%Y-%m-%d %H:%M:%S",
        xcom_strategy: str = 'file',
        file_format: str = 'parquet',
        **kwargs
    ) -> pd.DataFrame | dict | str:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
            dag_id_column (str, optional): Nom de la colonne pour l'ID du DAG. Defaults to "tech_dag_id".
            execution_date_column (str, optional): Nom de la colonne pour la date d'exécution. Defaults to "tech_execution_date".
            date_format (str, optional): Format de la date d'exécution. Defaults to "%Y-%m-%d %H:%M:%S".
            xcom_strategy (str, optional): Stratégie de stockage XCom ('direct', 'file'). Defaults to 'file'.
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

        if 'dag' not in kwargs:
            raise KeyError("❌ 'dag' absent du contexte Airflow")

        data = manager.Xcom.get(xcom_source, skip_if_empty=True, **kwargs)

        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"❌ tech_info nécessite un DataFrame, reçu: {type(data)}")

        # Copie pour éviter les effets de bord
        df_xcom = data.copy()

        execution_date = AddColumns.__get_execution_date(**kwargs)

        df_xcom[dag_id_column] = kwargs['dag'].dag_id
        df_xcom[execution_date_column] = execution_date.strftime(date_format)

        logging.info(f"✅ Colonnes '{dag_id_column}' et '{execution_date_column}' ajoutées.")

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
        date_format: str = "%Y-%m-%d",
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **kwargs
    ) -> pd.DataFrame | dict | str:
        """ Ajoute des colonnes techniques à un DataFrame extrait d'un XCom.

        Args:
            xcom_source (str): Identifiant de la tâche source pour extraire le DataFrame
            date_column (str, optional): Nom de la colonne pour la date d'exécution. Defaults to "tech_date_photo".
            year_column (str, optional): Nom de la colonne pour l'année d'exécution. Defaults to "tech_annee_photo".
            week_column (str, optional): Nom de la colonne pour la semaine d'exécution. Defaults to "tech_semaine_photo".
            date_format (str, optional): Format de la date d'exécution. Defaults to "%Y-%m-%d".
            xcom_strategy (str, optional): Stratégie de stockage XCom ('auto', 'direct', 'file'). Defaults to 'auto'.
            file_format (str, optional): Format de fichier pour le stockage XCom ('json' ou 'parquet'). Defaults to 'parquet'.

        Returns:
            pd.DataFrame | dict | str: Donnée avec les colonnes techniques ajoutées, sous forme de DataFrame, dictionnaire ou chaîne de caractères selon les paramètres.
        """

        # Récupération et validation
        data = manager.Xcom.get(xcom_source, skip_if_empty=True, **kwargs)

        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"❌ tech_photo nécessite un DataFrame, reçu: {type(data)}")

        # Copie pour éviter les effets de bord
        df_xcom = data.copy()

        execution_date = AddColumns.__get_execution_date(**kwargs)

        # Calcul des valeurs ISO
        iso_year, iso_week, _ = execution_date.isocalendar()

        # Ajout des colonnes
        df_xcom[date_column] = execution_date.strftime(date_format)
        df_xcom[year_column] = iso_year
        df_xcom[week_column] = iso_week

        logging.info(
            f"✅ Colonnes '{date_column}', '{year_column}' et '{week_column}' ajoutées "
            f"(semaine {iso_week}/{iso_year})."
        )

        return manager.Xcom.put(
            input=df_xcom,
            xcom_strategy=xcom_strategy,
            file_format=file_format,
            **kwargs
        )