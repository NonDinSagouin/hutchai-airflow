import pandas as pd
import logging

from sqlalchemy.engine import Engine
from sqlalchemy import text
from airflow.exceptions import AirflowFailException

import app.helper as helper
import app.manager as manager

from app.tasks.decorateurs import customTask

class Warehouse():

    @staticmethod
    def __setup_schema(engine: Engine, schema: str):
        """ Crée un schéma dans la base de données s'il n'existe pas déjà."""

        logging.info(f"⏳ Vérification/création du schéma '{schema}'")

        with engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            logging.info(f"✅ Schéma '{schema}' vérifié/créé avec succès.")

    @staticmethod
    def __setup_table(engine: Engine, table_name: str, schema: str):
        """ Crée une table dans la base de données s'il n'existe pas déjà."""

        logging.info(f"⏳ Vérification/création de la table '{schema}.{table_name}'")

        with engine.connect() as connection:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    id SERIAL PRIMARY KEY
                )
            """))
            logging.info(f"✅ Table '{schema}.{table_name}' vérifiée/créée avec succès.")

    @customTask
    @staticmethod
    def insert(
        xcom_source : str,
        engine : Engine,
        table_name: str = "test_table",
        schema: str = "public",
        chunksize: int = 1000,
        method : str = None,
        if_table_exists: str = "append",
        **kwargs
    ) -> None:
        """ Insère des données dans un entrepôt de données (Data Warehouse).

        Args:
            xcom_source (str): Identifiant de la tâche Airflow dont les données seront extraites via XCom.
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table dans laquelle insérer les données. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            chunksize (int, optional): Nombre de lignes à insérer par lot. Par défaut 1000.
            method (str, optional): Méthode d'insertion. Par défaut None.
            if_table_exists (str, optional): Comportement si la table existe déjà ("fail", "replace", "append"). Par défaut "append".

        Returns:
            None

        Examples:
            >>> Warehouse.insert(
            ...     xcom_source="extract_task",
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="public",
            ...     chunksize=500,
            ...     method=None,
            ...     if_table_exists="append"
            ... )
            Insère les données extraites par la tâche "extract_task" dans la table "public.sales_data" du Data Warehouse.
        """

        df = manager.Xcom.get(xcom_source=xcom_source, to_df=True, **kwargs)

        if df.empty:
            logging.warning("❌ Aucune donnée à traiter pour le snapshot")
            return

        Warehouse.__setup_schema(engine, schema)
        Warehouse.__setup_table(engine, table_name, schema)

        logging.info(f"⏳ Insertion de {len(df)} ligne(s) dans la table '{schema}.{table_name}'")

        try:
            # engine = manager.Connectors.postgres("POSTGRES_warehouse")
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            logging.error(f"❌ Erreur lors de l'insertion dans le Data Warehouse: {e}")
            raise

        logging.info(f"✅ Insertion terminée avec succès dans la table '{schema}.{table_name}'")

    @customTask
    @staticmethod
    def snapshot_by_period(
        xcom_source: str,
        engine: Engine,
        table_name: str = "test_table",
        schema: str = "public",
        annee_column: str = "tech_annee_photo",
        semaine_column: str = "tech_semaine_photo",
        chunksize: int = 1000,
        method: str = None,
        **kwargs
    ) -> None:
        """ Effectue une prise de photo (snapshot) des données par période (année/semaine)
        en supprimant les données existantes pour les périodes concernées puis en insérant les nouvelles.

        Args:
            xcom_source (str): Identifiant de la tâche Airflow dont les données seront extraites via XCom.
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table cible. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            annee_column (str, optional): Nom de la colonne année. Par défaut "annee".
            semaine_column (str, optional): Nom de la colonne semaine. Par défaut "num_semaine".
            chunksize (int, optional): Nombre de lignes à insérer par lot. Par défaut 1000.
            method (str, optional): Méthode d'insertion. Par défaut None.

        Examples:
            >>> Warehouse.snapshot_by_period(
            ...     xcom_source="extract_task",
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="public",
            ...     annee_column="year",
            ...     semaine_column="week",
            ...     chunksize=500,
            ...     method=None
            ... )
            Effectue un snapshot des données extraites par la tâche "extract_task" dans la table "public.sales_data",
            en supprimant les données existantes pour les périodes (année/semaine) concernées avant d'insérer les nouvelles.
        """

        df = manager.Xcom.get(xcom_source=xcom_source, to_df=True, **kwargs)

        if df.empty:
            logging.warning("Aucune donnée à traiter pour le snapshot")
            return

        # Vérifier que les colonnes nécessaires existent
        required_columns = [annee_column, semaine_column]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise AirflowFailException(f"Colonnes manquantes dans les données: {missing_columns}")

        Warehouse.__setup_schema(engine, schema)
        Warehouse.__setup_table(engine, table_name, schema)

        try:
            with engine.connect() as connection:
                # Récupérer les périodes uniques des nouvelles données
                periodes = df[[annee_column, semaine_column]].drop_duplicates()

                logging.info(f"Snapshot pour {len(periodes)} période(s) différente(s)")

                # Construire la clause WHERE pour supprimer les données existantes
                delete_conditions = []
                for _, periode in periodes.iterrows():
                    annee = periode[annee_column]
                    semaine = periode[semaine_column]
                    delete_conditions.append(f"({annee_column} = {annee} AND {semaine_column} = {semaine})")

                if delete_conditions:
                    where_clause = " OR ".join(delete_conditions)
                    delete_query = f"DELETE FROM {schema}.{table_name} WHERE {where_clause}"

                    logging.info("Suppression des données existantes pour les périodes concernées")
                    result = connection.execute(text(delete_query))
                    logging.info(f"{result.rowcount} ligne(s) supprimée(s)")

                # Insérer les nouvelles données
                logging.info(f"Insertion de {len(df)} nouvelle(s) ligne(s)")
                df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=connection,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    method=method
                )

                logging.info(f"Snapshot terminé avec succès pour la table {schema}.{table_name}")

        except Exception as e:
            logging.error(f"Erreur lors du snapshot dans le Data Warehouse: {e}")
            raise