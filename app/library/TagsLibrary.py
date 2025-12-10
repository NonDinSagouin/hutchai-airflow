class TagsLibrary():
    """
    Cette classe contient des constantes de chaîne de caractères utilisées comme tags pour différentes opérations de traitement de données et interactions avec des bases de données spécifiques.

    Attributs:
        DATA_INSERTION (str): Tag pour l'insertion de données.
        DATA_TRANSFORMATION (str): Tag pour la transformation de données.
        DATA_EXPORT (str): Tag pour l'exportation de données.
        DATA_IMPORT (str): Tag pour l'importation de données.
        GCP (str): Tag pour les interactions avec Google Cloud Platform.
        GCS (str): Tag pour les interactions avec Google Cloud Storage.
        DB_A2PO (str): Tag pour les interactions avec la base de données A2PO.
        DB_NOISETTE (str): Tag pour les interactions avec la base de données Noisette.
        DB_USERS (str): Tag pour les interactions avec la base de données des utilisateurs.

    exemple:
        >>> print(TagsLibrary.DATA_INSERTION)
        data_insertion
    """

    # Définissez des tags comme des constantes de classe
    DATA_EXTRACTION : str = "data_extraction"
    DATA_INSERTION : str = "data_insertion"
    DATA_TRANSFORMATION : str = "data_transformation"

    DATA_EXPORT : str = "data_export"
    DATA_IMPORT : str = "data_import"

    # Tags pour les opérations spécifiques
    API : str = "api"