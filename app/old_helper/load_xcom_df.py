import logging
import pandas

from pathlib import Path

def load_xcom_df(
    context,
    xcom_task_id : str,
    have_empty_security: bool = False,
) -> pandas.DataFrame:
    """ Charge un DataFrame Pandas à partir des données stockées dans XCom.

    Args:
        context (dict): Contexte d'exécution du DAG.
        xcom_task_id (str): ID de la tâche XCom à partir de laquelle charger les données.
        have_empty_security (bool): Indique si une sécurité contre les DataFrames vides est

    Returns:
        pandas.DataFrame: DataFrame Pandas
    """

    # Lire la DataFrame depuis XCom
    table_data = context.xcom_pull(task_ids=xcom_task_id)

    # Charger le CSV
    if table_data is None:
        raise ValueError(f"❌ Aucune donnée récupérée depuis XCom pour {xcom_task_id}.")

    logging.info(f"🖥️ Chargement du fichier du dataframe depuis xcom terminé : {xcom_task_id} ({len(table_data)} lignes)")

    if have_empty_security and table_data.empty :
        logging.warning("⚠️ Le fichier dataframe est vide. Arrêt de l'exécution.")
        return

    logging.info("🖥️ Chargement du fichier XCom terminé")
    # Informations sur le DataFrame
    num_rows, num_cols = table_data.shape
    logging.info(f"📊 Nombre de lignes : {num_rows}, Nombre de colonnes : {num_cols}")
    logging.info(f"📊 Liste des colonnes : {table_data.columns.tolist()}")
    logging.info(f"📊 Aperçu des premières lignes :\n{table_data.head().to_string()}")

    return table_data