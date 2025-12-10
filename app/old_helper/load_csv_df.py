import logging
import pandas

from pathlib import Path
from app.helper.load_file_params import load_file_params
from airflow.exceptions import AirflowFailException
from app.helper.logging_title import logging_title

def load_csv_df(
    dag_id : str,
    csv_file : str,
    dtype: dict = None,
    encoding : str = 'utf-8',
    have_empty_security: bool = False,
) -> pandas.DataFrame:
    """ Charge un fichier CSV en DataFrame Pandas avec vérification de l'existence du fichier et logs détaillés.

    Args:
        dag_id (str): Identifiant du DAG.
        csv_file (str): Nom du fichier CSV à charger.
        dtype (dict, optional): Dictionnaire spécifiant les types des colonnes.. Defaults to None.
        encoding (str, optional): Encodage du fichier CSV. Defaults to 'utf-8'.
        have_empty_security (bool, optional): Si True, arrête l'exécution si le fichier est vide.. Defaults to False.

    Returns:
        pandas.DataFrame: DataFrame Pandas
    """

    logging_title(f"Chargement du fichier CSV : {csv_file}", 3)

    params = load_file_params(dag_id)

    # Ajout de la gestion des types de colonnes
    if dtype is None:
        dtype = params['col_types']
    else:
        for key, value in params['col_types'].items():
            if key not in dtype:
                dtype[key] = value

    logging.info(f"🔍 Chargement du fichier CSV : {csv_file} avec les types de colonnes : {dtype}")

    csv_file_path = get_file_path(dag_id, csv_file)

    try:
        df_csv = pandas.read_csv(csv_file_path, dtype=dtype, encoding=encoding)
        logging.info(f"📑 Chargement du fichier CSV terminé : {csv_file_path} ({len(df_csv)} lignes)")

    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors du chargement du fichier CSV : {csv_file_path} - {e}")

    if have_empty_security and df_csv.empty :
        raise FileNotFoundError("⚠️ Le fichier CSV est vide. Arrêt de l'exécution.")

    # Informations sur le DataFrame
    num_rows, num_cols = df_csv.shape
    logging.info(f"📊 Nombre de lignes : {num_rows}, Nombre de colonnes : {num_cols}")
    logging.info(f"📊 Liste des colonnes : {df_csv.columns.tolist()}")
    logging.info(f"📊 Aperçu des premières lignes :\n{df_csv.head().to_string()}")

    logging_title(f"Chargement du fichier CSV terminé : {csv_file}", 3, close=True)

    return df_csv

def get_file_path(
    dag_id : str,
    csv_file : str,
) -> Path:

    csv_file = csv_file if csv_file.endswith(".csv") else csv_file + ".csv"
    csv_file_path = Path(f"./temp/{dag_id}/{csv_file}")

    # Charger le CSV
    if not csv_file_path.exists():
        raise FileNotFoundError(f"❌ Le fichier CSV n'existe pas : {csv_file_path}")

    return csv_file_path