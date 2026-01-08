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

    Examples:
        >>> call_api(
        ...     url="https://api.example.com/data",
        ...     headers={"Authorization": "Bearer token"},
        ...     raise_on_error=True
        ... )
        ✅ Données récupérées avec succès: 100 éléments.
        [{'id': 1, 'value': 'A'}, {'id': 2, 'value': 'B'}, ...]
    """

    logging.info(f"⏳ Appel de l'API: {url} ...")

    try:
        response = requests.get(url, headers=headers)

        if raise_on_error: response.raise_for_status()
        elif response.status_code == 429:
            if raise_on_error:
                raise AirflowFailException("❌ Trop de requêtes: Limite de taux dépassée.")
            else:
                logging.error("❌ Trop de requêtes: Limite de taux dépassée.")
                return None
        elif response.status_code != 200:
            logging.warning(f"⚠️ Status code non-200: {response.status_code}")
            return None

        try: data = response.json()
        except ValueError:
            if raise_on_error:
                raise AirflowFailException("❌ La réponse n'est pas au format JSON.")
            else:
                logging.error("❌ La réponse n'est pas au format JSON.")
                return None

    except requests.exceptions.RequestException as e:

        if raise_on_error:
            raise AirflowFailException(f"❌ Erreur lors de l'appel API: {e}")
        else:
            logging.error(f"❌ Erreur lors de l'appel API: {e}")
            return None

    logging.info(f"✅ Données récupérées avec succès: {len(data)} éléments.")
    return data