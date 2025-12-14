import requests
import logging

from typing import Any

def call_api(
        url,
        headers
) -> Any:
    """ Permet d'appeler une API REST et de gérer les erreurs.

    Args:
        url (str): URL de l'API à appeler.
        headers (dict): En-têtes HTTP à inclure dans la requête.
        
    Returns:
        Any: Données JSON récupérées depuis l'API.
    """
    
    try:    
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        logging.info(f"✅ Données récupérées avec succès: {len(data)} éléments.")

    except requests.exceptions.RequestException as e:
        raise Exception(f"❌ Erreur lors de l'appel API: {e}")
    
    return data