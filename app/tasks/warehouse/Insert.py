import pandas as pd
import logging

from sqlalchemy.engine import Engine
from sqlalchemy import text
from airflow.exceptions import AirflowFailException

import app.manager as manager
from app.tasks.decorateurs import customTask

class Insert():

    @staticmethod
    def __get_df(**kwargs) -> pd.DataFrame:
        """ Récupère un DataFrame à partir des données stockées dans XCom."""

        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids=kwargs['xcom_source'])

        df = pd.DataFrame(data)

        if df.empty:
            raise AirflowFailException("Le DataFrame est vide. Aucune donnée à récupérer depuis XCom.")

        return df
    
    @staticmethod
    def __setup_schema(engine: Engine, schema: str):
        """ Crée un schéma dans la base de données s'il n'existe pas déjà."""
        
        with engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            logging.info(f"Schéma '{schema}' vérifié/créé avec succès.")


    @customTask
    @staticmethod
    def basic(
        xcom_source : str,
        engine : Engine,
        table_name: str = "test_table",
        schema: str = "public",
        chunksize: int = 1000,
        method : str = None,
        if_table_exists: str = "append",
        **kwargs
    ):
        """ Insère des données dans un entrepôt de données (Data Warehouse).

        Args:
            xcom_source (str): Identifiant de la tâche Airflow dont les données seront extraites via XCom.
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table dans laquelle insérer les données. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            chunksize (int, optional): Nombre de lignes à insérer par lot. Par défaut 1000.
            method (str, optional): Méthode d'insertion. Par défaut None.
            if_table_exists (str, optional): Comportement si la table existe déjà ("fail", "replace", "append"). Par défaut "append".
        """

        df = Insert.__get_df(xcom_source=xcom_source, **kwargs)

        Insert.__setup_schema(engine, schema)

        try:
            # engine = manager.Connectors.postgres("POSTGRES_warehouse")
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            logging.error(f"Erreur lors de l'insertion dans le Data Warehouse: {e}")
            raise