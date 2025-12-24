class TagsLibrary():
    """ Cette classe contient des constantes de chaîne de caractères utilisées
    comme tags pour différentes opérations de traitement de données et interactions
    avec des bases de données spécifiques.

    Args:
        DATA_EXTRACTION (str): Tag pour les opérations d'extraction de données.
        DATA_INSERTION (str): Tag pour les opérations d'insertion de données.
        DATA_TRANSFORMATION (str): Tag pour les opérations de transformation de données.
        DATA_EXPORT (str): Tag pour les opérations d'exportation de données.
        DATA_IMPORT (str): Tag pour les opérations d'importation de données.
        API (str): Tag pour les opérations liées aux interactions avec des API.

    Examples:
        >>> print(TagsLibrary.DATA_INSERTION)
        data_insertion
    """

    # Jeux vidéo
    LEAGUE_OF_LEGENDS : str = "league_of_legends"
    RIOT_GAMES : str = "riot_games"

    # Stockage de données
    WAREHOUSE : str = "warehouse"
    DATA_ROWS : str = "data_rows" # données brutes
    DATA_FACT : str = "data_fact" # données factuelles
    DATA_DIMENSION : str = "data_dimension" # données dimensionnelles

    # Définissez des tags comme des constantes de classe
    DATA_EXTRACTION : str = "data_extraction"
    DATA_INSERTION : str = "data_insertion"
    DATA_TRANSFORMATION : str = "data_transformation"

    DATA_EXPORT : str = "data_export"
    DATA_IMPORT : str = "data_import"

    # Tags pour les opérations spécifiques
    API : str = "api"