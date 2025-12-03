import logging
import pandas as pd

from airflow.exceptions import AirflowFailException

import app.manager as manager

from app.tasks.decorateurs import customTask

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

        try:
            engine = manager.Connectors.postgres("POSTGRES_warehouse")
            df.to_sql(name=table_name, schema=schema ,con=engine, if_exists=if_table_exists, index=False, chunksize=chunksize, method=method,)

        except Exception as e:
            logging.error(f"Erreur lors de l'insertion dans le Data Warehouse: {e}")
            raise

    @customTask
    @staticmethod
    def spark_df(
        **kwargs
    ) -> pd.DataFrame:
        """Crée un DataFrame Spark et le convertit en pandas DataFrame."""

        try:
            spark = manager.SparkSessionManager.get(**kwargs)

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
            manager.SparkSessionManager.close(**kwargs)