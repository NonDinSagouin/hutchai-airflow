import os
import json
import logging

from airflow.exceptions import AirflowFailException

def load_file_params(dag_id):
    """Charge un fichier JSON de dates et le convertit en dictionnaire.

    Args:
        dag_id (int): Identifiant du DAG.
    """

    filename = f"./temp/{dag_id}/params.json"

    if not os.path.exists(filename):
        raise AirflowFailException(f"❌ Le fichier JSON {filename} n'existe pas. Il faut que la task Set_params initialise le fichier de configuration !")

    try:
        with open(filename, "r", encoding="utf-8") as f:
            params = json.load(f)
            logging.info(f"✅ Fichier JSON chargé avec succès: {filename}")
            logging.info(f"📌 Params date : {params['useful_dates']['today']}")
            logging.info(f"📌 Params id_flux : {params['id_flux']}")
            logging.info(f"📌 Params code_ui : {params['code_ui']}")
            return params

    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors du chargement du fichier JSON: {e}")