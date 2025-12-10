import logging

def logging_title(text: str, lvl: int = 1, close: bool = False):
    """ Enregistre un titre formaté dans les logs avec plusieurs niveaux.

    Args:
        text (str): Le texte à enregistrer dans les logs.
        lvl (int, optionnel): Le niveau du titre (1-5). Par défaut à 1.
            - 1: Titre principal (=== haut/bas)
            - 2: Sous-titre (--- haut/bas)
            - 3: Section (*** ouverture/fermeture)
            - 4: Sous-section (+++ ouverture/fermeture)
            - 5: Paragraphe (... haut uniquement)

    Examples:
        >>> logging_title(
        ...     "Démarrage du processus",
        ...     lvl=1
        ... )
        =============================================
        Démarrage du processus
        =============================================

        >>> logging_title(
        ...    "Chargement des données",
        ...    lvl=3
        ... )
        ▼ ********** Chargement des données ********** ▼

        >>> logging_title(
        ...    "Fin du traitement",
        ...    lvl=1,
        ...    close=True
        ... )
        Fin du traitement
        ▲ =========================================== ▲
    """

    if not isinstance(text, str):
        raise ValueError("Le paramètre 'text' doit être une chaîne de caractères.")
    if lvl not in [1, 2, 3, 4, 5]:
        raise ValueError("Le paramètre 'lvl' doit être compris entre 1 et 5.")

    # Configuration des patterns selon le niveau
    patterns = {
        1: {"char": "=", "length": 100},    # Titre principal
        2: {"char": "-", "length": 80},     # Sous-titre
        3: {"char": "*", "length": 10},     # Section
        4: {"char": "+", "length": 10},     # Sous-section
        5: {"char": ".", "length": 5}       # Paragraphe
    }

    config = patterns[lvl]
    paterne = config["char"] * config["length"]

    logger = logging.getLogger(__name__)

    if close:
        logger.info(text)
        logger.info( "▲ " + (paterne * 3) + " ▲")
        return

    if lvl in [3, 4, 5]:
        text = "▼ " + paterne + " " + text + " " + paterne + " ▼"
        logger.info(text)
        return

    logger.info(paterne)
    logger.info(text)
    logger.info(paterne)

