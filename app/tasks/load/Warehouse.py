import pandas as pd
import logging
import re

from typing import Any
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

        if not schema or not schema.strip():
            raise ValueError("Le nom du schéma ne peut pas être vide")

        if not re.match(r'^[a-zA-Z0-9_]+$', schema):
            raise ValueError(f"Nom de schéma invalide: '{schema}'")

        logging.info(f"⏳ Vérification/création du schéma '{schema}'")

        try:
            with engine.begin() as connection:
                connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                logging.info(f"✅ Schéma '{schema}' vérifié/créé avec succès.")

        except Exception as e:
            logging.error(f"❌ Erreur lors de la création du schéma '{schema}': {e}")
            raise AirflowFailException(f"Impossible de créer le schéma '{schema}': {e}")

    @staticmethod
    def __setup_table(
        engine: Engine,
        table_name: str,
        schema: str,
        columns: dict = None
    ):
        """ Crée une table dans la base de données s'il n'existe pas déjà."""

        if not table_name or not table_name.strip():
            raise ValueError("Le nom de la table ne peut pas être vide")

        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            raise ValueError(f"Nom de table invalide: '{table_name}'")

        if not re.match(r'^[a-zA-Z0-9_]+$', schema):
            raise ValueError(f"Nom de schéma invalide: '{schema}'")

        logging.info(f"⏳ Vérification/création de la table '{schema}.{table_name}'")

        try:
            with engine.connect() as connection:
                connection.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                        {', '.join([f'{col} {dtype}' for col, dtype in (columns or {}).items()])}
                    )
                """))
                logging.info(f"✅ Table '{schema}.{table_name}' vérifiée/créée avec succès.")

        except Exception as e:
            logging.error(f"❌ Erreur lors de la vérification de la table '{schema}.{table_name}': {e}")
            raise AirflowFailException(f"Impossible de vérifier la table: {e}") from e

    @customTask
    @staticmethod
    def extract(
        engine: Engine,
        table_name: str = None,
        schema: str = "public",
        shema_select: str = None,
        shema_where: str = None,
        shema_order: str = None,
        limit: int = None,
        **kwargs
    ) -> Any:
        """ Extrait des données d'un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table à partir de laquelle extraire les données. Par défaut None.
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            shema_select (list, optional): Liste des colonnes à sélectionner. Par défaut None (toutes les colonnes).
            shema_where (dict, optional): Dictionnaire des conditions WHERE pour filtrer les données. Par défaut None (aucun filtre).
            shema_order (str, optional): Colonne pour ordonner les résultats. Par défaut None (pas d'ordre).
            limit (int, optional): Limite le nombre de lignes extraites. Par défaut None (aucune limite).

        Returns:
            Any: Données extraites sous forme de DataFrame ou autre format selon l'implémentation.

        Examples:
            >>> Warehouse.extract(
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="toto",
            ...     shema_select=["id", "amount", "date"],
            ...     shema_where={"region": "North", "year": 2023}
            ... )
            Extrait les colonnes "id", "amount" et "date" de la table "sales_data" pour les enregistrements où la région est "North" et l'année est 2025.
        """
        query = f"SELECT * FROM {schema}.{table_name} "

        if shema_select:
            select_columns = ', '.join(shema_select)
            query = f"SELECT {select_columns} FROM {schema}.{table_name} "

        if shema_where:
            where_conditions = ' AND '.join([f"{col} = :{col}" for col in shema_where.keys()])
            query += f"WHERE {where_conditions}"

        if shema_order:
            query += f" ORDER BY {shema_order}"

        if limit:
            query += f" LIMIT {limit}"


        logging.info(f"⏳ Execution de la requête: {query} ...")

        with engine.begin() as connection:

            result = connection.execute(text(query), **(shema_where or {}))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        logging.info(f"✅ Requête exécutée avec succès, {len(df)} lignes extraites.")

        return manager.Xcom.put(
            input=df,
            **kwargs
        )

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
    ) -> dict:
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
            dict: Dictionnaire contenant le nom de la table, le schéma et le nombre de lignes insérées.

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

        df = manager.Xcom.get(xcom_source=xcom_source, **kwargs)

        if df.empty:
            raise AirflowFailException("Aucune donnée à insérer dans le Data Warehouse.")

        Warehouse.__setup_schema(engine, schema)

        logging.info(f"⏳ Insertion de {len(df)} ligne(s) dans la table '{schema}.{table_name}'")

        try:
            # engine = manager.Connectors.postgres("POSTGRES_warehouse")
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            logging.error(f"❌ Erreur lors de l'insertion dans le Data Warehouse: {e}")
            raise

        logging.info(f"✅ Insertion terminée avec succès dans la table '{schema}.{table_name}'")

        return {
            'table_name': table_name,
            'schema': schema,
            'rows_inserted': len(df),
        }

    @customTask
    @staticmethod
    def update(
        engine: Engine,
        table_name: str = "test_table",
        schema: str = "public",
        set_values: dict = None,
        where_conditions: dict = None,
        **kwargs
    ) -> dict:
        """ Met à jour des données dans un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table à mettre à jour. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            set_values (dict): Dictionnaire des colonnes et valeurs à mettre à jour.
            where_conditions (dict): Dictionnaire des conditions WHERE pour filtrer les lignes à mettre à jour.

        Returns:
            dict: Dictionnaire contenant le nom de la table, le schéma et le nombre de lignes mises à jour.

        Examples:
            >>> Warehouse.update(
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="public",
            ...     set_values={"amount": 200.0},
            ...     where_conditions={"id": 1}
            ... )
            Met à jour la colonne "amount" à 200.0 dans la table "publica.sales_data" pour l'enregistrement où l'id est 1.
        """

        if not set_values:
            raise ValueError("Aucune valeur spécifiée pour la mise à jour.")

        if not where_conditions:
            raise ValueError("Aucune condition WHERE spécifiée pour la mise à jour.")
        
        print(where_conditions["puuid"])

        # Fonctions SQL natives qui ne doivent pas être paramétrées
        SQL_FUNCTIONS = ['CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'NOW()', 'NULL']
        
        # Construction de la clause SET en distinguant les fonctions SQL des valeurs
        set_parts = []
        parameters = {}
        
        for col, val in set_values.items():
            if isinstance(val, str) and val.upper() in SQL_FUNCTIONS:
                # Fonction SQL native - insérer directement
                set_parts.append(f"{col} = {val}")
            else:
                # Valeur paramétrable
                set_parts.append(f"{col} = :set_{col}")
                parameters[f"set_{col}"] = val
        
        set_clause = ', '.join(set_parts)
        
        # Construction de la clause WHERE
        where_parts = []
        for col, val in where_conditions.items():
            # Extraire la valeur si c'est une Series pandas
            if hasattr(val, 'iloc'):
                val = val.iloc[0] if len(val) > 0 else val
            where_parts.append(f"{col} = :where_{col}")
            parameters[f"where_{col}"] = val
        
        where_clause = ' AND '.join(where_parts)

        update_query = f"""
            UPDATE {schema}.{table_name}
            SET {set_clause}
            WHERE {where_clause}
        """

        logging.info(f"⏳ Exécution de la requête de mise à jour: {update_query}")

        try:
            with engine.begin() as connection:
                result = connection.execute(text(update_query), **parameters)
                logging.info(f"✅ Mise à jour terminée avec succès dans la table '{schema}.{table_name}'. Lignes affectées: {result.rowcount}")

        except Exception as e:
            logging.error(f"❌ Erreur lors de la mise à jour dans le Data Warehouse: {e}")
            raise

        return {
            'table_name': table_name,
            'schema': schema,
            'rows_updated': result.rowcount,
        }

    @customTask
    @staticmethod
    def insert_lines(
        engine: Engine,
        table_name: str = "test_table",
        schema: str = "public",
        lines: list = None,
        **kwargs
    ) -> dict:
        """ Insère des lignes spécifiques dans une table d'un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table dans laquelle insérer les lignes. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            lines (list, optional): Liste des dictionnaires représentant les lignes à insérer.

        Returns:
            dict: Dictionnaire contenant le nom de la table, le schéma et le nombre de lignes insérées.

        Examples:
            >>> Warehouse.insert_lines(
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="public",
            ...     lines=[
            ...         {"id": 1, "amount": 100.0, "date": "2023-01-01"},
            ...         {"id": 2, "amount": 150.0, "date": "2023-01-02"}
            ...     ]
            ... )
            Insère deux lignes spécifiques dans la table "public.sales_data" du Data Warehouse.
        """

        if not lines:
            logging.warning("❌ Aucune ligne à insérer")
            return

        Warehouse.__setup_schema(engine, schema)
        Warehouse.__setup_table(engine, table_name, schema)

        try:
            with engine.begin() as connection:

                for row in lines:
                    columns = ', '.join(row.keys())
                    values = ', '.join([f":{key}" for key in row.keys()])
                    insert_query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({values})"
                    connection.execute(text(insert_query), **row)

            logging.info(f"✅ {len(lines)} ligne(s) insérée(s) avec succès dans la table '{schema}.{table_name}'")

        except Exception as e:
            logging.error(f"❌ Erreur lors de l'insertion des lignes dans le Data Warehouse: {e}")
            raise

        return {
            'table_name': table_name,
            'schema': schema,
            'rows_inserted': len(lines),
        }

    @customTask
    @staticmethod
    def create_table(
        engine: Engine,
        table_name: str = "test_table",
        schema: str = "public",
        columns: dict = None,
        **kwargs
    ) -> dict:
        """ Crée une table fact dans un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table à créer. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".

        Returns:
            None
        """

        Warehouse.__setup_schema(engine, schema)
        Warehouse.__setup_table(engine, table_name, schema, columns)

        logging.info(f"✅ Table '{schema}.{table_name}' prête dans le Data Warehouse.")

        return {
            'table_name': table_name,
            'schema': schema,
            'columns': columns or {},
        }

    @customTask
    @staticmethod
    def raw_to_fact(
        engine: Engine,
        outlets: list = None,
        source_table: str = None,
        target_table: str = None,
        join_keys: list = None,
        match_columns: dict = None,
        non_match_columns: dict = None,
        has_matched: bool = False,
        has_not_matched: bool = False,
        **kwargs
    ) -> dict:
        """ Transforme des données brutes en données factuelles dans un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            outlets (list, optional): Liste des assets de sortie pour le suivi des données.
            source_table (str): Nom de la table source contenant les données brutes.
            target_table (str): Nom de la table cible pour les données factuelles.
            schema (str, optional): Schéma de la base de données. Par défaut "public
            join_keys (list, optional): Liste des colonnes utilisées pour faire le lien entre les tables source et cible.
            match_columns (dict, optional): Dictionnaire des colonnes à mettre à jour en cas de correspondance.
            non_match_columns (dict, optional): Dictionnaire des colonnes à insérer en cas de non-correspondance.
            has_matched (bool, optional): Indique si les lignes correspondantes doivent être mises à jour. Par défaut False.
            has_not_matched (bool, optional): Indique si les lignes non correspondantes doivent être insérées. Par défaut False.

        Returns:
            None
        """

        if has_matched is False and has_not_matched is False:
            raise ValueError("Aucune opération de transformation spécifiée (has_matched et has_not_matched sont tous deux False). Aucune action effectuée.")

        transformation_result = {
            'source_table': source_table,
            'target_table': target_table,
            'operations_performed': [],
            'rows_affected': 0,
            'timestamp': pd.Timestamp.now().isoformat(),
            'outlets_materialized': outlets or [],
        }

        try:
            with engine.begin() as connection:

                matched = ""
                not_matched = ""

                if has_matched:
                    matched = f"""
                    WHEN MATCHED THEN
                        UPDATE SET
                            {', '.join([f'fact.{col} = raw.{col}' for col in match_columns.keys()])},
                            tech_date_modification = CURRENT_TIMESTAMP
                    """
                    transformation_result['operations_performed'].append('UPDATE')

                if has_not_matched:
                    not_matched = f"""
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(non_match_columns.keys())}, tech_date_creation, tech_date_modification)
                        VALUES ({', '.join([f'raw.{col}' for col in non_match_columns.values()])}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                    transformation_result['operations_performed'].append('INSERT')

                transform_query = f"""
                    MERGE INTO {target_table} AS fact
                    USING {source_table} AS raw
                    ON {" AND ".join([f"fact.{key} = raw.{key}" for key in join_keys])}

                    {matched}
                    {not_matched};
                """

                logging.info(f"⏳ Transformation des données brutes en données factuelles dans la table '{target_table}_fact'")
                logging.info(f"Requête de transformation: {transform_query}")

                result = connection.execute(text(transform_query))
                transformation_result['rows_affected'] = result.rowcount

                logging.info(f"✅ Transformation terminée avec succès dans la table '{target_table}_fact'. Lignes affectées: {result.rowcount}")

        except Exception as e:
            logging.error(f"❌ Erreur lors de la transformation dans le Data Warehouse: {e}")
            transformation_result['error'] = str(e)
            raise

        return transformation_result