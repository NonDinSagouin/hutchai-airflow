import os
import logging

def load_sql_file(file_path: str):
    """Charge et retourne le contenu d'un fichier SQL. (path de base 'app/sql/')

    Args:
        file_path (str): Chemin du fichier SQL à lire.

    Returns:
        str: Contenu du fichier SQL.
    """

    file_path = f"app/sql/{file_path}"

    if not file_path.lower().endswith(".sql"):
        raise ValueError("❌ Seuls les fichiers SQL (.sql) sont autorisés.")

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"❌ Le fichier '{file_path}' est introuvable.")

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()
    except IOError as e:
        raise IOError(f"❌ Erreur lors de la lecture du fichier '{file_path}': {e}")