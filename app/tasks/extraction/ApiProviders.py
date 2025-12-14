import logging
import requests
import pandas as pd

import app.manager as manager
import app.helper as helper
from app.tasks.decorateurs import customTask

class ApiProviders():

    @customTask
    @staticmethod
    def get(
        conn_id : str = None,
        endpoint: str = None,
        to_dataframe: bool = True,
        xcom_strategy: str = 'auto',
        file_format: str = 'parquet',
        **kwargs
    ) -> pd.DataFrame | dict | str:
        """ Tâche basique d'exemple qui imprime un message de bienvenue.

        Args:
            conn_id (str, optional): Identifiant de la connexion API. Defaults to None.
            endpoint (str, optional): Endpoint de l'API à appeler. Defaults to None.
            to_dataframe (bool, optional): Convertir les données en DataFrame pandas. Defaults to True.
            xcom_strategy (str, optional): Stratégie de stockage XCom ('auto', 'direct', 'file'). Defaults to 'auto'.
            file_format (str, optional): Format de fichier pour le stockage XCom ('json' ou 'parquet'). Defaults to 'parquet'.

        Returns:
            pd.DataFrame | dict | str: Données récupérées depuis l'API, sous forme de DataFrame, dictionnaire ou chaîne de caractères selon les paramètres.

        Examples:
            >>> ApiProviders.get(
            ...     conn_id="my_api_connection",
            ...     endpoint="/data",
            ...     to_dataframe=True,
            ...     xcom_strategy="auto",
            ...     file_format="parquet"
            ... )
            ⏳ Appel API vers: https://api.example.com/data
            ✅ Données récupérées avec succès: 150 éléments.
        """

        if conn_id is None:
            raise ValueError("❌ La connexion ID 'conn_id' doit être fournie.")

        if endpoint is None:
            raise ValueError("❌ L'endpoint de l'API doit être fourni.")

        http_details = manager.Connectors.http(conn_id)

        url = f"{http_details['host']}{endpoint}"
        logging.info(f"⏳ Appel API vers: {url}")

        data = helper.call_api(
            url=url,
            headers=http_details.get('headers', {})
        )

        if to_dataframe:
            data = pd.json_normalize(data)
            logging.info(f"✅ Données converties en DataFrame pandas avec succès. Dimensions: {data.shape}.")
        
        return manager.Xcom.put(
            input=data,
            xcom_strategy=xcom_strategy,
            file_format=file_format,
            **kwargs
        )