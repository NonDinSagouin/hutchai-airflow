import pandas as pd
import os
import logging
import json

from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable

import app.helper as helper

class Xcom:

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

        if not xcom_source:
            raise AirflowFailException("❌ Le paramètre 'xcom_source' est obligatoire pour récupérer les données depuis XCom.")
        if 'ti' not in context:
            raise AirflowFailException("TaskInstance manquante dans le contexte")

        helper.logging_title("Récupération des données depuis XCom", lvl=3)

        ti = context.get('ti')
        if not ti:
            raise AirflowFailException("❌ TaskInstance introuvable dans le contexte")
        data = ti.xcom_pull(task_ids=xcom_source)

        # Si c'est un chemin de fichier, le charger
        if isinstance(data, str) and os.path.isfile(data):
            logging.info(f"⏳ Chargement du fichier depuis XCom: {data}")

            # Déterminer le format selon l'extension
            if data.endswith('.parquet'):
                try:
                    data = pd.read_parquet(data)
                except Exception as e:
                    raise AirflowFailException(f"❌ Erreur lecture parquet: {str(e)}")
                
                logging.info(f"✅ Fichier parquet chargé avec succès")

            elif data.endswith('.json'):
                try:
                    with open(data, 'r') as f: data = json.load(f)
                except Exception as e:
                    raise AirflowFailException(f"❌ Erreur lecture JSON: {str(e)}")
                
                logging.info(f"✅ Fichier JSON chargé avec succès")

            else:
                raise AirflowFailException(f"❌ Format de fichier non supporté")

        if isinstance(data, pd.DataFrame):
            helper.logging_title(f"✅ DataFrame récupéré depuis XCom avec {data.shape[0]} lignes et {data.shape[1]} colonnes.", lvl=3, close=True)

        elif isinstance(data, dict):
            helper.logging_title(f"✅ Dict récupéré depuis XCom avec {len(data)} clés.", lvl=3, close=True)

        else:
            helper.logging_title(f"✅ String récupéré depuis XCom avec {len(data)} caractères.", lvl=3, close=True)

        return data

    @staticmethod
    def put(
        input: str | pd.DataFrame | dict,
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **context
    ) -> str | pd.DataFrame | dict:
        """ Prépare les données pour le stockage dans XCom selon la stratégie choisie.

        Args:
            input (str | pd.DataFrame | dict): Données à stocker dans XCom
            xcom_strategy (str, optionnel): Stratégie de stockage ('direct', 'file', 'auto'). Par défaut à 'auto'.
            file_format (str, optionnel): Format de fichier si stratégie 'file' ('json' ou 'parquet'). Par défaut à 'parquet'.
            **context: Contexte Airflow contenant TaskInstance

        Returns:
            str | pd.DataFrame | dict: Données à stocker dans XCom (chemin de fichier ou données directes)

        Examples:
            >>> filepath = Xcom.put(
            ...     input=large_dataframe,
            ...     xcom_strategy='auto',
            ...     file_format='parquet',
            ...     **context
            ... )
            # Si le DataFrame est volumineux, il sera sauvegardé dans un fichier et le chemin sera retourné.
            >>> print(filepath)
            /tmp/airflow_data/task_123_20240601_153045_123456.parquet
        """

        if xcom_strategy not in ['direct', 'file', 'auto']:
            raise AirflowFailException("❌ Le paramètre 'xcom_strategy' doit être 'direct', 'file' ou 'auto'.")
        
        if file_format not in ['json', 'parquet']:
            raise AirflowFailException("❌ Le paramètre 'file_format' doit être 'json' ou 'parquet'.")

        if 'ti' not in context:
            raise AirflowFailException("TaskInstance manquante dans le contexte")

        ti = context.get('ti')
        if not ti:
            raise AirflowFailException("❌ TaskInstance introuvable dans le contexte")

        helper.logging_title("Préparation des données pour XCom", lvl=3)
        tmp_folder = Variable.get("Folder_tmp_data", default="/tmp/airflow_data")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        output = None

        # Stratégie adaptative selon la taille
        if xcom_strategy == 'auto':
            # Si c'est un DataFrame et > 100KB, utiliser fichier
            if isinstance(input, pd.DataFrame) and input.memory_usage(deep=True).sum() > 100 * 1024:
                xcom_strategy = 'file'
            # Si c'est un dict ou string volumineux, utiliser fichier
            elif isinstance(input, (dict, str)) and len(str(input)) > 100 * 1024:
                xcom_strategy = 'file'
            else:
                xcom_strategy = 'direct'

        logging.info(f"ℹ️ Stratégie XCom utilisée: {xcom_strategy}")

        if xcom_strategy == 'file':

            task_id = ti.task_id
            
            # Déterminer l'extension selon le format et le type de données
            if file_format == 'json': output = Xcom.__file_strategy_json(input, tmp_folder, task_id, timestamp)
            else: output = Xcom.__file_strategy_parquet(input, tmp_folder, task_id, timestamp)

            logging.info(f"✅ Données sauvegardées dans: {output}")

        elif xcom_strategy == 'direct':
            logging.info(f"✅ Données prêtes pour stockage direct dans XCom")
            output = input

        Xcom.clean_tmp_files(tmp_folder=tmp_folder, older_than_minutes=60)
        helper.logging_title(f"✅ Données préparées pour XCom.", lvl=3, close=True)
        return output

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
        filepath = f"{tmp_folder}/{task_id}_{timestamp}.json"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if isinstance(input, pd.DataFrame):
            logging.info("⏳ Sauvegarde du DataFrame en JSON")
            input.to_json(filepath, orient='records', index=False)

        elif isinstance(input, dict):
            logging.info("⏳ Sauvegarde du dict en JSON")
            with open(filepath, 'w', encoding='utf-8') as f: json.dump(input, f, ensure_ascii=False, indent=2)

        else:
            logging.info("⏳ Sauvegarde du string en JSON")
            with open(filepath, 'w', encoding='utf-8') as f: f.write(str(input))

        logging.info(f"💾 Fichier JSON sauvegardé avec succès")
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
    def clean_tmp_files(
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