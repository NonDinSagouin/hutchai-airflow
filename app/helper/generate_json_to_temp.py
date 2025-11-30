import os
import json
import logging

def generate_json_to_temp(dag_id, data: dict, file_name : str):
    """Génère un fichier JSON temporaire à partir d'un dictionnaire.

    Args:
        dag_id (int): Identifiant du DAG.
        data (dict): Données à enregistrer.
        file_name (str): Nom du fichier JSON à générer.
    """
    folder_temp = f"./temp/{dag_id}"

    try:
        os.makedirs(folder_temp, exist_ok=True)
    except OSError as e:
        logging.error(f"❌ Erreur lors de la création du répertoire temporaire: {e}")
        return

    if not data:
        logging.warning("⚠️ Les données sont vides. Aucune donnée générée.")

    else:
        filename = f"{folder_temp}/{file_name}.json"

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            logging.info(f"💾 Fichier JSON enregistré: {filename}")

        except Exception as e:
            logging.error(f"❌ Erreur lors de l'écriture du fichier JSON: {e}")