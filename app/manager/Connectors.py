import logging
import hashlib

from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.sdk.bases.hook import BaseHook

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

from app.helper import logging_title

class Connectors:

    # Cache pour les connexions
    _connection_cache: dict[str, Any] = {}

    @staticmethod
    def clear_cache():
        """Vide le cache des connexions"""
        Connectors._connection_cache.clear()

    @staticmethod
    def __generate_cache_key(method_name: str, **kwargs) -> str:
        """Génère une clé de cache unique basée sur le nom de la méthode et les paramètres

        Args:
            method_name (str): Nom de la méthode
            **kwargs: Paramètres de la méthode

        Returns:
            str: Clé de cache unique

        Example:
            >>> Connectors.__generate_cache_key("http", conn_id="my_conn")
            'a1b2c3d4e5f6g7h8i9j0...'
        """
        params_str = f"{method_name}" + "".join(f"_{k}_{v}" for k, v in sorted(kwargs.items()))
        return hashlib.sha256(params_str.encode()).hexdigest()

    @staticmethod
    def http(
        conn_id: str
    ) -> dict:
        """ Configure une connexion HTTP à partir d'un ID de connexion Airflow.

        Args:
            conn_id (str): ID de la connexion Airflow

        Returns:
            dict: Détails de la connexion HTTP
        """

        # Vérification du cache
        cache_key = Connectors.__generate_cache_key("http", conn_id=conn_id)
        if cache_key in Connectors._connection_cache: return Connectors._connection_cache[cache_key]

        logging_title("⏳ Configuration de la connexion HTTP", lvl=3)

        if not conn_id:
            raise ValueError("❌ Le paramètre 'conn_id' ne peut pas être vide.")

        try:
            conn = BaseHook.get_connection(conn_id)

            if conn.conn_type != "http":
                raise AirflowFailException(f"❌ La connexion {conn_id} n'est pas de type HTTP.")

            http_details = {
                "host": conn.host,
                "schema": conn.schema,
                "login": conn.login,
                "password": conn.password,
                "port": conn.port,
                "extra": conn.extra_dejson,
            }

            logging.info(f"✅ Connexion HTTP '{conn_id}' configurée avec succès.")
            logging.info("📌 Détails de la connexion HTTP: ")
            logging.info(f"🔹host: {http_details['host']}")
            logging.info(f"🔹schema: {http_details['schema']}")
            logging.info(f"🔹port: {http_details['port']}")

            logging_title("✅ Connexion", lvl=3, close=True)

            Connectors._connection_cache[cache_key] = http_details
            return http_details

        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de la configuration de la connexion HTTP: {e}") from e

    @staticmethod
    def postgres(
        conn_id: str
    ) -> Engine:
        """ Configure une connexion à une base de données PostgreSQL à partir d'un ID de connexion Airflow.

        Args:
            conn_id (str): ID de la connexion Airflow

        Returns:
            Engine: Moteur SQLAlchemy pour la connexion PostgreSQL
        """

        # Vérification du cache
        cache_key = Connectors.__generate_cache_key("postgres", conn_id=conn_id)
        if cache_key in Connectors._connection_cache: return Connectors._connection_cache[cache_key]

        logging_title("⏳ Configuration de la connexion PostgreSQL", lvl=3)

        if not conn_id:
            raise ValueError("❌ Le paramètre 'conn_id' ne peut pas être vide.")

        hook = PostgresHook(postgres_conn_id=conn_id)

        try:
            # Récupérer l'URL de la base de données à partir du hook
            conn_url = hook.get_uri()

            engine = create_engine(
                conn_url,
            )

        except SQLAlchemyError as e:
            # Échec de connexion
            raise AirflowFailException(f"❌ Échec de connexion à {hook.postgres_conn_id} ! {e}")

        conn = BaseHook.get_connection(conn_id)
        logging.info(f"✅ Connexion PostgreSQL '{conn_id}' configurée avec succès.")
        logging.info("📌 Détails de la connexion PostgreSQL: ")
        logging.info(f"🔹host: {conn.host}")
        logging.info(f"🔹schema: {conn.schema}")
        logging.info(f"🔹port: {conn.port}")
        logging_title("", lvl=3, close=True)

        Connectors._connection_cache[cache_key] = engine
        return engine