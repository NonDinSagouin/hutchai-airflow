import logging
import requests

import app.manager as manager

from app.tasks.decorateurs import customTask

class Providers():

    @customTask
    @staticmethod
    def api_get(
        conn_id : str = None,
        endpoint: str = None,
        task_id : str = 'task_API_get',
        **kwargs
    ) -> None:
        """ Tâche basique d'exemple qui imprime un message de bienvenue.

        Args:
            name (str, optional): Nom à saluer. Defaults to 'strenger'.
        """

        if conn_id is None:
            raise ValueError("La connexion ID 'conn_id' doit être fournie.")
        
        if endpoint is None:
            raise ValueError("L'endpoint de l'API doit être fourni.")

        http_details = manager.Connectors.http(conn_id)

        url = f"{http_details['host']}{endpoint}"
        logging.info(f"Appel API vers: {url}")

        try:
            response = requests.get(url, headers=http_details.get('headers'))
            response.raise_for_status()
            
            data = response.json()
            logging.info(f"Données récupérées avec succès: {len(data.get('data', {}))} éléments.")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de l'appel API: {e}")
            raise
