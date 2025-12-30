import requests
import logging

from typing import Any
from airflow.exceptions import AirflowFailException

def call_api(
        url,
        headers,
        raise_on_error: bool = True,
) -> Any:
    """ Permet d'appeler une API REST et de gérer les erreurs.

    Args:
        url (str): URL de l'API à appeler.
        headers (dict): En-têtes HTTP à inclure dans la requête.
        raise_on_error (bool): Si True, lève une exception en cas d'erreur HTTP. Défaut: True.
        
    Returns:
        Any: Données JSON récupérées depuis l'API.
    """
    
    try:    
        response = requests.get(url, headers=headers)
        
        if raise_on_error:
            response.raise_for_status()
        elif response.status_code != 200:
            logging.warning(f"⚠️ Status code non-200: {response.status_code}")
            return None

        data = response.json()
        logging.info(f"✅ Données récupérées avec succès: {len(data)} éléments.")

    except requests.exceptions.RequestException as e:
        
        if raise_on_error:
            raise AirflowFailException(f"❌ Erreur lors de l'appel API: {e}")
        else:
            logging.error(f"❌ Erreur lors de l'appel API: {e}")
            return None
    
    return data