import pandas as pd
import os
import logging
import json

from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable

import app.helper as helper

class Xcom:

    SUPPORTED_STRATEGIES = ['direct', 'file']
    SUPPORTED_FORMATS = ['json', 'parquet']

    @staticmethod
    def get(
        xcom_source : str,
        **context
    ) -> pd.DataFrame | dict | str:
        """ Récupère un DataFrame à partir des données stockées dans XCom.

        Args:
            xcom_source (str): ID de la tâche source des données XCom
            **context: Contexte Airflow contenant TaskInstance

        Returns:
            dict | pd.DataFrame | str: Données récupérées depuis XCom

        Examples:
            >>> data = Xcom.get(
            ...     xcom_source='extract_data_task',
            ...     **context
            ... )
            # Si les données sont volumineuses et stockées dans un fichier,
            # elles seront chargées automatiquement en DataFrame ou dict.
            >>> print(data.head())
                col1  col2
        """
        helper.logging_title("Récupération des données depuis XCom", lvl=3)

        if not xcom_source:
            raise AirflowFailException("❌ Le paramètre 'xcom_source' est obligatoire pour récupérer les données depuis XCom.")

        ti = Xcom.__get_context(context)
        data = ti.xcom_pull(task_ids=xcom_source)

        if data is None:
            raise AirflowFailException(f"❌ Aucune donnée trouvée pour '{xcom_source}'")

        processed_data = Xcom.__process_data(data)
        Xcom.__log_result(processed_data)

        return processed_data

    @staticmethod
    def put(
        input: str | pd.DataFrame | dict,
        xcom_strategy: str = 'file',
        file_format: str = 'parquet',
        **context
    ) -> str | pd.DataFrame | dict:
        """ Prépare les données pour le stockage dans XCom selon la stratégie choisie.

        Args:
            input (str | pd.DataFrame | dict): Données à stocker dans XCom
            xcom_strategy (str, optionnel): Stratégie de stockage ('direct', 'file'). Par défaut à 'file'.
            file_format (str, optionnel): Format de fichier si stratégie 'file' ('json' ou 'parquet'). Par défaut à 'parquet'.
            **context: Contexte Airflow contenant TaskInstance

        Returns:
            str | pd.DataFrame | dict: Données à stocker dans XCom (chemin de fichier ou données directes)

        Examples:
            >>> filepath = Xcom.put(
            ...     input=large_dataframe,
            ...     xcom_strategy='file',
            ...     file_format='parquet',
            ...     **context
            ... )
            # Le DataFrame est sauvegardé dans un fichier Parquet et le chemin est retourné.
            >>> print(filepath)
            /tmp/airflow_data/task_123_20240601_153045_123456.parquet
        """

        helper.logging_title(f"Préparation des données pour XCom. Format: {file_format}, Stratégie: {xcom_strategy}", lvl=3)

        if xcom_strategy not in Xcom.SUPPORTED_STRATEGIES:
            raise AirflowFailException("❌ Le paramètre 'xcom_strategy' doit être 'direct' ou 'file'.")

        if file_format not in Xcom.SUPPORTED_FORMATS:
            raise AirflowFailException("❌ Le paramètre 'file_format' doit être 'json' ou 'parquet'.")

        ti = Xcom.__get_context(context)

        tmp_folder = Variable.get("Folder_tmp_data", default="/tmp/airflow_data")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        output = None

        if xcom_strategy == 'file':

            task_id = ti.task_id

            # Déterminer l'extension selon le format et le type de données
            if file_format == 'json':
                output = Xcom.__file_strategy_json(input, tmp_folder, task_id, timestamp)
            elif file_format == 'parquet':
                output = Xcom.__file_strategy_parquet(input, tmp_folder, task_id, timestamp)
            else:
                raise AirflowFailException("❌ Format de fichier non supporté pour la stratégie 'file'.")

            logging.info(f"✅ Données sauvegardées dans: {output}")

        elif xcom_strategy == 'direct':
            logging.info("✅ Données prêtes pour stockage direct dans XCom")
            output = input

        Xcom.__clean_tmp_files(tmp_folder=tmp_folder, older_than_minutes=60)
        helper.logging_title("✅ Données préparées pour XCom.", lvl=3, close=True)
        return output

    @classmethod
    def __get_context(cls, context: dict) -> any:
        """Validation centralisée du contexte Airflow"""
        if 'ti' not in context:
            raise AirflowFailException("TaskInstance manquante dans le contexte")
        
        return context.get('ti')

    @staticmethod
    def __file_strategy_json(
        input: str | pd.DataFrame | dict,
        tmp_folder: str,
        task_id: str,
        timestamp: str,
    ):
        """ Sauvegarde les données dans un fichier JSON.

        Args:
            input (str | pd.DataFrame | dict): Données à sauvegarder
            tmp_folder (str): Dossier temporaire pour sauvegarder le fichier
            task_id (str): ID de la tâche Airflow
            timestamp (str): Timestamp pour nommer le fichier

        Returns:
            str: Chemin du fichier sauvegardé
        """
        try:
            filepath = f"{tmp_folder}/{task_id}_{timestamp}.json"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        except Exception as e:
            raise AirflowFailException(f"❌ Erreur création dossier temporaire: {str(e)}") from e

        if isinstance(input, pd.DataFrame):
            try:
                input.to_json(filepath, orient='records', index=False)
            except Exception as e:
                raise AirflowFailException(f"❌ Erreur sauvegarde DataFrame en JSON: {str(e)}") from e

        elif isinstance(input, dict):
            try:
                with open(filepath, 'w', encoding='utf-8') as f: json.dump(input, f, ensure_ascii=False, indent=2)
            except Exception as e:
                raise AirflowFailException(f"❌ Erreur sauvegarde dict en JSON: {str(e)}") from e

        else:
            try:
                with open(filepath, 'w', encoding='utf-8') as f: f.write(str(input))
            except Exception as e:
                raise AirflowFailException(f"❌ Erreur sauvegarde string en JSON: {str(e)}") from e

        logging.info("💾 Fichier JSON sauvegardé avec succès")
        return filepath

    @staticmethod
    def __file_strategy_parquet(
        input: pd.DataFrame,
        tmp_folder: str,
        task_id: str,
        timestamp: str,
    ):
        """ Sauvegarde les données dans un fichier Parquet.

        Args:
            input (pd.DataFrame): DataFrame à sauvegarder
            tmp_folder (str): Dossier temporaire pour sauvegarder le fichier
            task_id (str): ID de la tâche Airflow
            timestamp (str): Timestamp pour nommer le fichier

        Returns:
            str: Chemin du fichier sauvegardé
        """

        logging.info("⏳ Sauvegarde du DataFrame en Parquet")
        filepath = f"{tmp_folder}/{task_id}_{timestamp}.parquet"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        input.to_parquet(filepath, index=False)

        logging.info(f"💾 Fichier Parquet sauvegardé avec succès")
        return filepath

    @staticmethod
    def __log_result(
        result: str | pd.DataFrame | dict
    ):
        """ Log le résultat selon son type.

        Args:
            result (str | pd.DataFrame | dict): Résultat à logger
        """

        if isinstance(result, pd.DataFrame):
            helper.logging_title(f"✅ DataFrame avec {result.shape[0]} lignes et {result.shape[1]} colonnes.", lvl=3, close=True)

        elif isinstance(result, dict):
            helper.logging_title(f"✅ Dict avec {len(result)} clés.", lvl=3, close=True)

        elif isinstance(result, str):
            helper.logging_title(f"✅ String avec {len(result)} caractères.", lvl=3, close=True)

        else:
            raise AirflowFailException(f"❌ Type de données non supporté pour le logging: {type(result)}")

    @staticmethod
    def __process_data(
        data: str | pd.DataFrame | dict
    ) -> pd.DataFrame | dict | str:
        """ Traite les données récupérées depuis XCom.

        Args:
            data (str | pd.DataFrame | dict): Données récupérées depuis XCom

        Returns:
            pd.DataFrame | dict | str: Données traitées
        """

        SUCCESS_LOG = "✅ Données récupérées et traitées depuis XCom."

        # Si c'est un chemin de fichier, le charger
        if isinstance(data, str) and os.path.isfile(data):
            logging.info(f"⏳ Chargement du fichier depuis XCom: {data}")

            # Déterminer le format selon l'extension
            if data.endswith('.parquet'):
                try:
                    data = pd.read_parquet(data)
                except Exception as e:
                    raise AirflowFailException(f"❌ Erreur lecture parquet: {str(e)}")

                logging.info(SUCCESS_LOG)

            elif data.endswith('.json'):
                try:
                    with open(data, 'r') as f: data = json.load(f)
                except Exception as e:
                    raise AirflowFailException(f"❌ Erreur lecture JSON: {str(e)}")

                logging.info(SUCCESS_LOG)

            else:
                raise AirflowFailException("❌ Format de fichier non supporté")

        if data is None:
            raise AirflowFailException("❌ Aucune donnée trouvée dans XCom source.")
        
        logging.info(SUCCESS_LOG)
        
        return data

    @staticmethod
    def __clean_tmp_files(
        tmp_folder: str,
        older_than_minutes: int = 60
    ):
        """ Nettoie les fichiers temporaires plus anciens qu'un certain temps.

        Args:
            tmp_folder (str): Dossier temporaire à nettoyer
            older_than_minutes (int, optionnel): Supprimer les fichiers plus anciens que ce nombre de minutes. Par défaut à 60.
        """

        now = datetime.now()
        cutoff = now.timestamp() - (older_than_minutes * 60)
        deleted_count = 0

        if not os.path.exists(tmp_folder):
            logging.info(f"✅ Nettoyage terminé: le dossier temporaire '{tmp_folder}' n'existe pas.")
            return

        for filename in os.listdir(tmp_folder):
            filepath = os.path.join(tmp_folder, filename)
            if os.path.isfile(filepath):
                file_mtime = os.path.getmtime(filepath)
                if file_mtime < cutoff:
                    os.remove(filepath)
                    logging.info(f"🗑️ Fichier supprimé: {filepath}")
                    deleted_count += 1

        if deleted_count > 0:
            logging.info(f"✅ Nettoyage terminé: {deleted_count} fichier(s) supprimé(s).")
        else:
            logging.info("✅ Nettoyage terminé: aucun fichier à supprimer.")