from sqlalchemy.engine import Engine
from airflow.datasets import Dataset
from urllib.parse import parse_qs

def get_sqlserver_dataset(engine: Engine, table: str, schema: str = "dbo") -> Dataset:
    """ Génère un Dataset Airflow au format sqlserver://server/database.schema.table
    à partir d'un SQLAlchemy Engine utilisant mssql+pyodbc ou mssql+pymssql.

    Args:
        engine (Engine): Instance SQLAlchemy de la connexion SQL Server.
        table (str): Nom de la table.
        schema (str, optional): Nom du schéma. Defaults to "dbo".

    Returns:
        Dataset: Instance de Dataset Airflow.
    """
    try:
        url = engine.url

        if url.drivername.startswith("mssql+pyodbc"):
            odbc_params = dict(url.query) if isinstance(url.query, dict) else {}
            database = url.database or odbc_params.get("Database", ["default_db"])[0]

        elif url.drivername.startswith("mssql+pymssql"):
            database = url.database or "default_db"

        else:
            raise ValueError(f"Driver SQL Server non supporté : {url.drivername}")

        dataset_uri = f"sqlserver://{database}/{schema}/{table}"

        return Dataset(dataset_uri)

    except Exception as e:
        raise ValueError(f"Erreur lors de la génération du Dataset SQL Server: {e}")