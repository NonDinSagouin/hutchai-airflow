import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.sdk.bases.hook import BaseHook

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

from app.helper import logging_title

class Connectors:

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

        logging_title("⏳ Configuration de la connexion HTTP", lvl=3)

        try:
            conn = BaseHook.get_connection(conn_id)

            if conn.conn_type != "http":
                raise AirflowFailException(f"La connexion {conn_id} n'est pas de type HTTP.")

            http_details = {
                "host": conn.host,
                "schema": conn.schema,
                "login": conn.login,
                "password": conn.password,
                "port": conn.port,
                "extra": conn.extra_dejson,
            }

            logging.info(f"✅ Connexion HTTP '{conn_id}' configurée avec succès.")
            logging.info("Détails de la connexion HTTP: ")
            logging.info(f"host: {http_details['host']}")
            logging.info(f"schema: {http_details['schema']}")
            logging.info(f"port: {http_details['port']}")

            logging_title("", lvl=3, close=True)

            return http_details

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la configuration de la connexion HTTP: {e}") from e

    @staticmethod
    def postgres(
        conn_id: str
    ) -> Engine:

        hook = PostgresHook(postgres_conn_id=conn_id)

        try:
            # Récupérer l'URL de la base de données à partir du hook
            conn_url = hook.get_uri()

            engine = create_engine(
                conn_url,
            )

        except SQLAlchemyError as e:
            # Échec de connexion
            raise AirflowFailException(f"Échec de connexion à {hook.postgres_conn_id} ! {e}")

        return engine