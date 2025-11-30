import os
import json
import logging

def load_json_to_dict(dag_id, file_name):
    """Charge un fichier JSON et le convertit en dictionnaire.

    Args:
        dag_id (int): Identifiant du DAG.
        file_name (str): Nom du fichier JSON à charger.
    """

    filename = f"./temp/{dag_id}/{file_name}.json"

    if not os.path.exists(filename):
        logging.error(f"❌ Le fichier JSON {filename} n'existe pas.")
        return None

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            logging.info(f"✅ Fichier JSON chargé avec succès: {filename}")
            return data

    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement du fichier JSON: {e}")
        return None