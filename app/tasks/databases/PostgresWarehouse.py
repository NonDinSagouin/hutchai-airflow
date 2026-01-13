import pandas as pd
import logging
import re
import pendulum

from typing import Any
from datetime import datetime, timezone

from sqlalchemy.engine import Engine
from sqlalchemy import text
from airflow.exceptions import AirflowFailException, AirflowSkipException

import app.helper as helper
import app.manager as manager

from app.tasks.decorateurs import customTask

class PostgresWarehouse():

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
        columns: dict = None,
        primary_key: list = None
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
            # Construction des colonnes
            column_definitions = []
            if columns:
                for col, dtype in columns.items():
                    column_definitions.append(f'{col} {dtype}')

            # Ajout de la clé primaire si spécifiée
            if primary_key and len(primary_key) > 0:
                primary_key_str = ', '.join(primary_key)
                column_definitions.append(f'PRIMARY KEY ({primary_key_str})')

            columns_sql = ', '.join(column_definitions)

            with engine.connect() as connection:
                connection.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                        {columns_sql}
                    )
                """))
                logging.info(f"✅ Table '{schema}.{table_name}' vérifiée/créée avec succès.")

        except Exception as e:
            logging.error(f"❌ Erreur lors de la vérification de la table '{schema}.{table_name}': {e}")
            raise AirflowFailException(f"Impossible de vérifier la table: {e}") from e

    @staticmethod
    def __get_execution_date(**kwargs) -> pendulum.DateTime:
        """Méthode privée pour récupérer le DataFrame et la date d'exécution.

        Returns:
            pendulum.DateTime: Date d'exécution
        """
        PARIS_TZ = "Europe/Paris"

        if 'logical_date' in kwargs:
            execution_date = kwargs['logical_date'].in_timezone(PARIS_TZ)
        elif 'execution_date' in kwargs:
            execution_date = kwargs['execution_date'].in_timezone(PARIS_TZ)
        elif 'ds' in kwargs:
            execution_date = pendulum.parse(kwargs['ds']).in_timezone(PARIS_TZ)
        else:
            raise KeyError("❌ Aucune date d'exécution trouvée dans le contexte (logical_date, execution_date, ds)")

        logging.debug(f"📅 Date d'exécution récupérée : {execution_date}")
        return execution_date

    @customTask
    @staticmethod
    def extract(
        engine: Engine,
        table_name: str = None,
        schema: str = "public",
        schema_select: str = None,
        schema_where: str = None,
        schema_order: str = None,
        limit: int = None,
        skip_empty: bool = False,
        joins: list = None,
        **kwargs
    ) -> Any:
        """ Extrait des données d'un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table à partir de laquelle extraire les données. Par défaut None.
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            schema_select (list, optional): Liste des colonnes à sélectionner. Par défaut None (toutes les colonnes).
            schema_where (dict, optional): Dictionnaire des conditions WHERE pour filtrer les données. Par défaut None (aucun filtre).
            schema_order (str, optional): Colonne pour ordonner les résultats. Par défaut None (pas d'ordre).
            limit (int, optional): Limite le nombre de lignes extraites. Par défaut None (aucune limite).
            skip_empty (bool, optional): Si True, saute la tâche si aucune donnée n'est extraite. Par défaut False.
            joins (list, optional): (avec alias t_main pour la table principale) Liste des jointures à effectuer. Chaque élément est un dictionnaire avec les clés:
                - 'table': nom de la table à joindre (format: schema.table ou table)
                - 'on': condition de jointure (ex: "t1.id = t2.id")
                - 'type': type de jointure (INNER, LEFT, RIGHT, FULL). Par défaut INNER.
                - 'alias': alias pour la table jointe (optionnel)

        Returns:
            Any: Données extraites sous forme de DataFrame ou autre format selon l'implémentation.

        Examples:
            >>> PostgresWarehouse.extract(
            ...     engine=engine,
            ...     table_name="sales_data",
            ...     schema="public",
            ...     schema_select=["s.id", "s.amount", "c.name"],
            ...     joins=[
            ...         {
            ...             'table': 'customers',
            ...             'alias': 'c',
            ...             'on': 's.customer_id = c.id',
            ...             'type': 'INNER'
            ...         }
            ...     ],
            ...     schema_where={"s.year": 2023}
            ... )
            Extrait des données de sales_data avec une jointure INNER sur customers.
        """
        query = f"\nSELECT \n\t * \n\r FROM \n\t {schema}.{table_name} "

        # Gestion des alias pour la table principale
        main_table_alias = ""
        if joins:
            # Si on a des jointures, on ajoute un alias à la table principale pour éviter l'ambiguité
            main_table_alias = "t_main"
            query = f"\nSELECT \n\t * \n\r FROM \n\t {schema}.{table_name} AS {main_table_alias} "

        if schema_select:
            select_columns = '\n\t, '.join(schema_select)
            if joins:
                query = f"\nSELECT \n\t{select_columns} \nFROM \n\t{schema}.{table_name} AS {main_table_alias} "
            else:
                query = f"\nSELECT \n\t{select_columns} \nFROM \n\t{schema}.{table_name} "

        # Construction des jointures
        if joins:
            for join in joins:
                join_type = join.get('type', 'INNER').upper()
                join_table = join['table']
                join_on = join['on']
                join_alias = join.get('alias', '')
                
                # Ajouter le schéma par défaut si pas spécifié dans la table
                if '.' not in join_table:
                    join_table = f"{schema}.{join_table}"
                
                # Construction de la clause JOIN
                if join_alias:
                    query += f"\n{join_type} JOIN \n\t{join_table} AS {join_alias} \n\t\tON {join_on}"
                else:
                    query += f"\n{join_type} JOIN \n\t{join_table} \n\t\tON {join_on}"
        if schema_where:

            where_conditions = []
            params = {}
            for col, val in schema_where.items():
                # Si la valeur commence par "is" (is null, is not null, etc.), ne pas utiliser de paramètre
                if isinstance(val, str) and val.lower().startswith('is '):
                    where_conditions.append(f"\n\t {col} {val}")
                else:
                    # Sinon, utiliser un paramètre pour l'égalité
                    where_conditions.append(f"\n\t {col} = :{col}")
                    params[col] = val

            if where_conditions:
                query += "\nWHERE " + "\n\tAND ".join(where_conditions)

        if schema_order:
            query += f"\nORDER BY \n\t{schema_order}"

        if limit:
            query += f"\nLIMIT {limit}"

        logging.info(f"⏳ Execution de la requête: {query} ...")

        with engine.begin() as connection:

            result = connection.execute(text(query), **(schema_where or {}))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if df.empty:
            message = "⚠️ Aucune donnée extraite du Data Warehouse."

            if skip_empty:
                logging.warning(message + " La tâche sera sautée.")
                raise AirflowSkipException(message + " La tâche sera sautée.")

            logging.warning(message)

        logging.info(f"✅ Requête exécutée avec succès, {len(df)} lignes extraites.")

        return manager.Xcom.put(
            input=df,
            **kwargs
        )

    @customTask
    @staticmethod
    def extract_from_dict(
        xcom_source: str,
        key: str,
        **kwargs
    ) -> Any:
        """ Extrait un élément spécifique d'un dictionnaire stocké dans XCom.

        Args:
            xcom_source (str): Identifiant de la tâche Airflow dont le dictionnaire sera extrait via XCom.
            key (str): Clé de l'élément à extraire du dictionnaire.
            **kwargs: Contexte d'exécution Airflow.

        Returns:
            Any: L'élément extrait du dictionnaire (généralement un DataFrame).

        Examples:
            >>> PostgresWarehouse.extract_from_dict(
            ...     xcom_source="fetch_task",
            ...     key="match_data"
            ... )
            Extrait l'élément "match_data" du dictionnaire retourné par "fetch_task".
        """

        data = manager.Xcom.get(xcom_source=xcom_source, skip_if_empty=True, **kwargs)

        if not isinstance(data, dict):
            raise AirflowFailException(f"Les données récupérées ne sont pas un dictionnaire. Type reçu: {type(data)}")

        if key not in data:
            raise AirflowFailException(f"La clé '{key}' n'existe pas dans le dictionnaire. Clés disponibles: {list(data.keys())}")

        extracted_data = data[key]
        logging.info(f"✅ Élément '{key}' extrait avec succès du dictionnaire")
        logging.info(f"ℹ️ Nombre d'éléments extraits: {len(extracted_data) if hasattr(extracted_data, '__len__') else 'N/A'}")

        return manager.Xcom.put(
            input=extracted_data,
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
        add_technical_columns: bool = False,
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
            >>> PostgresWarehouse.insert(
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

        df = manager.Xcom.get(xcom_source=xcom_source, skip_if_empty=True, **kwargs)

        if not isinstance(df, pd.DataFrame):
            raise AirflowFailException(f"❌ Les données récupérées depuis '{xcom_source}' ne sont pas un DataFrame. Type reçu: {type(df)}")

        if df.empty:
            raise AirflowFailException("❌ Aucune donnée à insérer dans le Data Warehouse (DataFrame vide).")

        if add_technical_columns:
            execution_date = PostgresWarehouse.__get_execution_date(**kwargs)
            df["tech_dag_id"] = kwargs['dag'].dag_id
            df["tech_execution_date"] = execution_date.strftime("%Y-%m-%d %H:%M:%S")

        PostgresWarehouse.__setup_schema(engine, schema)

        logging.info(f"⏳ Insertion de {len(df)} ligne(s) dans la table '{schema}.{table_name}'")

        try:
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de l'insertion dans le Data Warehouse: {e}") from e

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
            >>> PostgresWarehouse.update(
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
        logging.info(f"Paramètres set : {set_values}")
        logging.info(f"Paramètres where : {where_conditions}")

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
            >>> PostgresWarehouse.insert_lines(
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

        PostgresWarehouse.__setup_schema(engine, schema)
        PostgresWarehouse.__setup_table(engine, table_name, schema)

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
        primary_key: list = None,
        **kwargs
    ) -> dict:
        """ Crée une table fact dans un entrepôt de données (Data Warehouse).

        Args:
            engine (Engine): Moteur SQLAlchemy pour la connexion à la base de données.
            table_name (str, optional): Nom de la table à créer. Par défaut "test_table".
            schema (str, optional): Schéma de la base de données. Par défaut "public".
            columns (dict, optional): Dictionnaire des colonnes et leurs types de données.
            primary_key (list, optional): Liste des colonnes constituant la clé primaire.

        Returns:
            None
        """

        PostgresWarehouse.__setup_schema(engine, schema)
        PostgresWarehouse.__setup_table(engine, table_name, schema, columns, primary_key)

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
                            {', '.join([f'{col} = raw.{col}' for col in match_columns.keys()])},
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