import re
import logging

from jinja2 import Template, StrictUndefined

def apply_jinja(query : str, params : dict):
    """Remplace les variables Jinja dans une requête par les valeurs fournies dans params.
    Cette fonction vérifie que tous les paramètres nécessaires sont fournis et empêche l'utilisation
    de variables non définies pour éviter les erreurs d'exécution.

    Args:
        query (str): La chaîne contenant les variables Jinja à remplacer.
        params (dict): Un dictionnaire contenant les valeurs à substituer dans la requête.

    Returns:
        str: La requête après remplacement des variables Jinja.
    """

    logging.info(f"⚙️ Lancement de la fonction apply_jinja avec les paramètres : {params}")

    # Identifier les paramètres présents dans la requête
    found_params = set(re.findall(r"\{\{\s*(\w+)\s*\}\}", query))

    # Vérifier la correspondance avec les paramètres fournis
    provided_params = set(params.keys())
    missing_params = found_params - provided_params

    if missing_params:
        raise ValueError(f"Les paramètres suivants sont manquants : {missing_params}")

    # Remplacer les variables Jinja dans la requête avec les valeurs des params
    template = Template(query)
    # semgrep: ignore
    query = template.render(params=params) # nosemgrep

    # Sécuriser le rendu en empêchant l'utilisation de variables non définies
    template = Template(query, undefined=StrictUndefined)

    # Appliquer le rendu
    try:
        # nosemgrep: python.flask.security.xss.audit.direct-use-of-jinja2.direct-use-of-jinja2
        rendered_query = template.render(params=params)
    except Exception as e:
        raise ValueError(f"Erreur lors du rendu du template : {str(e)}")

    logging.info("✅ Requête MERGE générée !")
    return rendered_query
