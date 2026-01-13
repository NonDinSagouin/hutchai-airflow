import logging
import pandas as pd

from airflow.exceptions import AirflowFailException

import app.manager as manager

from app.tasks.decorateurs import customTask

class Basic():
    """ Classe de tâches basiques d'exemple. """

    @customTask
    @staticmethod
    def hello(
        name : str = 'strenger',
        **kwargs
    ) -> None:
        """ Tâche basique d'exemple qui imprime un message de bienvenue.

        Args:
            name (str, optional): Nom à saluer. Defaults to 'strenger'.

        Returns:
            None

        Examples:
            >>> Basic.hello(
            ...     name="Alice",
            ...     **kwargs
            ... )
            ✅ Welcome Alice, from Basic task!
        """
        logging.info(f"✅ Welcome {name}, from Basic task!")

    @customTask
    @staticmethod
    def xcom_put(
        var_test: str,
        **kwargs
    ) -> dict:
        """ Tâche basique d'exemple qui pousse des données dans XCom.

        Args:
            var_test (str): Variable de test à afficher.

        Returns:
            dict: Dictionnaire de données à pousser dans XCom.

        Examples:
            >>> Basic.xcom_put(
            ...     var_test="TestValue",
            ...     **kwargs
            ... )
            ℹ️ Variable de test reçue: TestValue
            ✅ Données poussées dans XCom: {'example_key': 'example_value', 'number': 123, 'status': 'completed', 'var_test': 'TestValue'}
        """

        data = {
            "example_key": "example_value",
            "number": 123,
            "status": "completed",
            "var_test": var_test,
        }

        logging.info(f"ℹ️ Variable de test reçue: {var_test}")

        return manager.Xcom.put(
            input=data,
            xcom_strategy="auto",
            **kwargs
        )

    @customTask
    @staticmethod
    def spark_df(
        **kwargs
    ) -> pd.DataFrame:
        """Crée un DataFrame Spark et le convertit en pandas DataFrame.

        Returns:
            pd.DataFrame: Le DataFrame converti en pandas.

        Examples:
            >>> Basic.spark_df(
            ...     **kwargs
            ... )
            ✅ DataFrame Spark converti en pandas DataFrame avec succès.
        """

        try:
            spark = manager.Spark.get(**kwargs)

            # Créer un DataFrame simple
            df = spark.createDataFrame(
                [("Alice", 25), ("Bob", 30), ("Charlie", 35)],
                ["name", "age"]
            )
            df.show()

            # Convertir en pandas et retourner
            result = df.toPandas()
            logging.info("✅ DataFrame Spark converti en pandas DataFrame avec succès.")

            return manager.Xcom.put(
                input=result,
                xcom_strategy="file",
                file_format="parquet",
                **kwargs
            )
        finally:
            # Toujours fermer la session Spark
            manager.Spark.close(**kwargs)

    @customTask
    @staticmethod
    def xcom_get(
        xcom_source: str,
        **kwargs
    ) -> bool:
        """ Lit les données depuis XCom et les retourne.

        Args:
            xcom_source (str): L'ID de la tâche source pour lire les données XCom.

        Returns:
            bool: True si la lecture a réussi.

        Examples:
            >>> Basic.xcom_get(
            ...     xcom_source="previous_task_id",
            ...     **kwargs
            ... )
            ✅ Données lues depuis XCom: {'example_key': 'example_value', 'number': 123, 'status': 'completed', 'var_test': 'TestValue'}
        """

        data = manager.Xcom.get(
            xcom_source=xcom_source,
            skip_if_empty=True,
            **kwargs
        )

        if data is None:
            raise AirflowFailException("❌ Aucune donnée trouvée dans XCom.")

        logging.info(f"✅ Données lues depuis XCom: {data}")

        return True
