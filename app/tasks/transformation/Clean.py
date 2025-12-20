import pandas as pd
import logging
import pendulum

import app.manager as manager

from typing import Any
from app.tasks.decorateurs import customTask

class Clean():

    @customTask
    @staticmethod
    def drop_duplicates(
        xcom_source,
        subset_columns,
        **kwargs
    ) -> Any:
        """ Supprime les duplicats en fonction des colonnes spécifiées.

        Args:
            xcom_source (str): Source XCom de la tâche précédente.
            subset_columns (list): Liste des colonnes à considérer pour identifier les duplicats.

        Returns:
            Any: Données sans duplicats.
        """
        logging.info(f"🔄 Suppression des duplicats en fonction des colonnes : {subset_columns}")

        df = manager.Xcom.get(
            xcom_source=xcom_source,
            **kwargs
        )

        before_count = len(df)
        df = df.drop_duplicates(subset=subset_columns)
        after_count = len(df)

        logging.info(f"✅ Duplicat supprimés en fonction des colonnes : {subset_columns}")
        logging.info(f"ℹ️ Nombre de lignes avant : {before_count}, après : {after_count}, supprimés : {before_count - after_count}")

        return manager.Xcom.put(
            input=df,
            **kwargs
        )

    @customTask
    @staticmethod
    def keep_columns(
        xcom_source: str,
        columns_to_keep: list,
        **kwargs
    ) -> Any:
        """
        Keep only specified columns in the DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame.
            columns_to_keep (list): List of columns to retain.

        Returns:
            pd.DataFrame: DataFrame with only the specified columns.

        Example:
            df_cleaned = Clean.keep_columns(
                xcom_source="source_task_id",
                columns_to_keep=["col1", "col2", "col3"],
                **context
            )
            Dans cet exemple, seules les colonnes "col1", "col2" et "col3" seront conservées dans le DataFrame résultant.
        """
        logging.info(f"🔄 Conservation des colonnes : {columns_to_keep}")

        df = manager.Xcom.get(
            xcom_source=xcom_source,
            **kwargs
        )

        df = df[columns_to_keep]

        logging.info(f"✅ Colonnes conservées : {columns_to_keep}")

        return manager.Xcom.put(
            input=df,
            **kwargs
        )

    @customTask
    @staticmethod
    def rename_columns(
        xcom_source: str,
        columns_mapping: dict,
        **kwargs
    ) -> Any:
        """ Rename columns in the DataFrame based on a provided mapping.

        Args:
            df (pd.DataFrame): Input DataFrame.
            columns_mapping (dict): Dictionary mapping old column names to new column names.

        Returns:
            pd.DataFrame: DataFrame with renamed columns.

        Example:
            df_renamed = Clean.rename_columns(
                xcom_source="source_task_id",
                columns_mapping={"old_name1": "new_name1", "old_name2": "new_name2"},
                **context
            )
            Dans cet exemple, les colonnes "old_name1" et "old_name2" seront renommées en "new_name1" et "new_name2" respectivement.
        """
        logging.info(f"🔄 Renommage des colonnes selon le mapping : {columns_mapping}")

        df = manager.Xcom.get(
            xcom_source=xcom_source,
            **kwargs
        )

        df = df.rename(columns=columns_mapping)

        logging.info(f"✅ Colonnes renommées selon le mapping : {columns_mapping}")

        return manager.Xcom.put(
            input=df,
            **kwargs
        )