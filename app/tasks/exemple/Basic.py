import logging
import pandas as pd

from app.tasks.decorateurs import customTask
from airflow.exceptions import AirflowFailException

from airflow.sdk import task
from pyspark import SparkContext
from pyspark.sql import SparkSession

class Basic():

    @customTask
    @staticmethod
    def hello(
        name:str = 'strenger',
        **kwargs
    ) -> None:
        """ Tâche basique d'exemple qui imprime un message de bienvenue.

        Args:
            name (str, optional): Nom à saluer. Defaults to 'strenger'.
        """
        logging.info(f"Welcome {name}, from Basic task!")

    @customTask
    @staticmethod
    def xcom(
        var_test: str,
        **kwargs
    ) -> dict:
        """ Tâche basique d'exemple qui pousse des données dans XCom.

        Args:
            var_test (str): Variable de test à afficher.

        Returns:
            dict: Dictionnaire de données à pousser dans XCom.
        """

        ti = kwargs['task_instance']

        logging.info(f"Variable reçue : {var_test}")

        try:
            logging.info("Exécution de la tâche Basic")

            # Push de XComs spécifiques avec des clés
            ti.xcom_push(key="key1", value="Valeur pour key1")
            ti.xcom_push(key="key2", value={"data": "Valeur pour key2", "count": 42})
            ti.xcom_push(key="metadata", value=[1, 2, 3, 4, 5])

                # Retour principal (sera stocké avec key="return_value")
            return {
                "main_result": "Données principales",
                "status": "success",
                "processed_items": 10
            }

        except Exception as e:
            logging.error(f"Erreur dans Basic: {e}")
            raise

    @customTask
    @staticmethod
    def call_API(
        http_conn_id: str,
        endpoint: str,
        method: str = "GET",
        headers: dict = None,
        log_response: bool = True,
        **kwargs
    ):
        """ Permet d'appeler une API externe et de traiter la réponse.
        """
        import requests
        from airflow.hooks.base import BaseHook

        try:
            conn = BaseHook.get_connection(http_conn_id)
            host = conn.host

            url = f"{host}{endpoint}"

            response = requests.request(method, url, headers=headers)
            response.raise_for_status()

            if log_response: logging.info(f"Réponse de l'API: {response.text}")

            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de l'appel API: {e}")
            raise

    @customTask
    @staticmethod
    def insert_warehouse(
        df: list,
        table_name: str = "test_table",
        schema: str = "public",
        chunksize: int = 1000,
        method : str = None,
        if_table_exists: str = "append",
        **kwargs
    ):
        """ Insère des données dans un entrepôt de données (Data Warehouse).

        Args:
            data (list): Liste de dictionnaires représentant les données à insérer
            table_name (str): Nom de la table de destination
        """
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        try:
            hook = PostgresHook(postgres_conn_id="POSTGES_warehouse")
            engine = Basic.__set_connector(hook)
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            logging.error(f"Erreur lors de l'insertion dans le Data Warehouse: {e}")
            raise

    @staticmethod
    def __set_connector(hook):

        from sqlalchemy import create_engine
        from sqlalchemy.exc import SQLAlchemyError

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

    @staticmethod
    def __set_spark_session(
        conn_id: str = "spark_default",
        app_name: str = "AirflowSparkTask"
    ):
        """Configure et retourne une SparkSession à partir d'une connexion Airflow."""
        import socket
        from pyspark.sql import SparkSession
        from airflow.hooks.base import BaseHook
        from airflow.exceptions import AirflowFailException
        
        try:
            # Récupérer la connexion Spark depuis Airflow
            conn = BaseHook.get_connection(conn_id)
            host = conn.host
            port = conn.port
            
            # Récupérer les configurations depuis extra (JSON)
            extra = conn.extra_dejson
            executor_memory = extra.get("executor-memory", "2g")
            executor_cores = extra.get("executor-cores", "2")
            total_executor_cores = extra.get("total-executor-cores", "4")
            
            # Obtenir l'adresse IP du conteneur
            hostname = socket.gethostname()
            host_ip = socket.gethostbyname(hostname)
            
            logging.info(f"Driver hostname: {hostname}, IP: {host_ip}")
            logging.info(f"Connecting to Spark cluster at spark://{host}:{port}")
            
            # Créer la session Spark
            spark = SparkSession.builder \
                .appName(app_name) \
                .master(f"spark://{host}:{port}") \
                .config("spark.driver.host", host_ip) \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.executor.cores", executor_cores) \
                .config("spark.cores.max", total_executor_cores) \
                .config("spark.driver.memory", "1g") \
                .getOrCreate()
            
            return spark
            
        except Exception as e:
            raise AirflowFailException(f"Échec de connexion au cluster Spark {conn_id} ! {e}")

    @customTask
    @staticmethod
    def spark_df(
        **kwargs
    ) -> pd.DataFrame:
        """Crée un DataFrame Spark et le convertit en pandas DataFrame."""
        
        try:
            spark = Basic.__set_spark_session()
            
            # Créer un DataFrame simple
            df = spark.createDataFrame(
                [("Alice", 25), ("Bob", 30), ("Charlie", 35)],
                ["name", "age"]
            )
            df.show()
            
            # Convertir en pandas et retourner
            result = df.toPandas()
            return result
        finally:
            # Toujours fermer la session Spark
            spark.stop()