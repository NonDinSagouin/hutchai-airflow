import os
import logging
import pandas

from airflow.exceptions import AirflowFailException
from app.helper.logging_title import logging_title
from io import BytesIO

def generate_csv_to_temp(
    dag_id : str,
    data : pandas.DataFrame | bytes,
    file_name : str,
    output_separator : str = ",",
    byte_separator : str = ",",
    empty_security : bool = False,
    dtype : dict = None,
    ):
    """Génère un fichier CSV temporaire à partir de data (dataframe ou byte).
    Il est possible d'enregistrer le fichier dans un sous-répertoire du DAG. exemple : "download/OJS_download_{file}.csv"

    Args:
        dag_id (str):  Identifiant du DAG.
        data (pandas.DataFrame | bytes): DataFrame à enregistrer.
        file_name (str): Nom du fichier CSV à générer.
        output_separator (str, optional): Séparateur pour le fichier CSV. Par défaut ","
        byte_separator (str, optional): Séparateur pour le CSV à partir des bytes. Par défaut ",".
        empty_security (bool, optional): Si True, lève une exception si le DataFrame est vide. Par défaut False.
        dtype (dict, optional): Retypage des colonnes souhaitées. Par défaut "None".
    """

    logging_title(f"📁 Génération du fichier CSV temporaire pour le DAG {dag_id} avec le nom de fichier {file_name}.csv", 3)

    if not isinstance(data, (bytes, pandas.DataFrame)):
        raise AirflowFailException("❌ Le paramètre 'data' doit être un objet de type 'bytes' ou 'pandas.DataFrame'.")

    folder_temp = f"./temp/{dag_id}"
    file_path = f"{folder_temp}/{file_name}.csv"

    # Extraire le répertoire du nom de fichier s'il en contient un
    file_dir = os.path.dirname(file_path)

    try:
        os.makedirs(file_dir, exist_ok=True)
    except OSError as e:
        raise AirflowFailException(f"❌ Erreur lors de la création du répertoire temporaire pour le DAG {dag_id}: {e}")

    if isinstance(data, bytes):
        try:
            # Utilisation de BytesIO pour simuler un fichier à partir des bytes
            data_io = BytesIO(data)
            data = pandas.read_csv(data_io, sep=byte_separator, dtype=dtype)  # Utilisation du séparateur pour lire le CSV
        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de la lecture des bytes en DataFrame: {e}")

    if data.empty:
        logging.warning("⚠️ Le DataFrame est vide. Aucune donnée générée.")
    else:

        # Informations sur le DataFrame
        num_rows, num_cols = data.shape
        logging.info(f"📊 Nombre de lignes : {num_rows}, Nombre de colonnes : {num_cols}")

        try:
            data.to_csv(file_path, index=False, na_rep="null", sep=output_separator)
            logging_title(f"💾 Fichier CSV enregistré: {file_path}", 3, close=True)
        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de l'écriture du fichier CSV: {e}")

    if empty_security and data.empty:
        raise AirflowFailException("❌ Le fichier CSV est vide.")

    return file_path