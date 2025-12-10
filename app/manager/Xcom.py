import pandas as pd
import os
import logging
import json

from datetime import datetime

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable

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
        """

        ti = context['ti']
        data = ti.xcom_pull(task_ids=xcom_source)

        # Si c'est un chemin de fichier, le charger
        if isinstance(data, str) and os.path.isfile(data):
            logging.info(f"Chargement du fichier depuis XCom: {data}")

            # Déterminer le format selon l'extension
            if data.endswith('.parquet'):
                data_loaded = pd.read_parquet(data)
            elif data.endswith('.json'):
                with open(data, 'r') as f: data_loaded = json.load(f)
            else:
                raise AirflowFailException(f"Format de fichier non supporté: {data}")

            return data_loaded

        return data

    @staticmethod
    def put(
        input: str | pd.DataFrame | dict,
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **kwargs
    ):
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

        logging.info(f"Stratégie XCom utilisée: {xcom_strategy}")

        if xcom_strategy == 'file':

            task_id = kwargs['ti'].task_id
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            tmp_folder = Variable.get("Folder_tmp_data", default="/tmp/airflow_data")

            # Déterminer l'extension selon le format et le type de données
            if file_format == 'json': filepath = Xcom.__file_strategy_json(input, tmp_folder, task_id, timestamp)
            else: filepath = Xcom.__file_strategy_parquet(input, tmp_folder, task_id, timestamp)

            logging.info(f"Données sauvegardées dans: {filepath}")
            return filepath

        return input

    @staticmethod
    def __file_strategy_json(
        input: str | pd.DataFrame | dict,
        tmp_folder: str,
        task_id: str,
        timestamp: str,
    ):
        filepath = f"{tmp_folder}/{task_id}_{timestamp}.json"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if isinstance(input, pd.DataFrame):
            input.to_json(filepath, orient='records', index=False)

        elif isinstance(input, dict):
            with open(filepath, 'w', encoding='utf-8') as f: json.dump(input, f, ensure_ascii=False, indent=2)

        else:  # string
            with open(filepath, 'w', encoding='utf-8') as f: f.write(str(input))

        return filepath

    @staticmethod
    def __file_strategy_parquet(
        input: str | pd.DataFrame | dict,
        tmp_folder: str,
        task_id: str,
        timestamp: str,
    ):
        if not isinstance(input, pd.DataFrame):
            raise AirflowFailException(f"Format parquet nécessite un DataFrame, reçu: {type(input)}")

        filepath = f"{tmp_folder}/{task_id}_{timestamp}.parquet"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        input.to_parquet(filepath, index=False)

        return filepath