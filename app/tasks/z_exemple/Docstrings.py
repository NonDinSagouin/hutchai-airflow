# Style google pour le docstring Python
# Référence : https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings
# Ce docstring suit les conventions de style Google pour la documentation des fonctions Python.


# Sections disponibles :
#   - Args: paramètres d'entrée
#   - Returns: valeur(s) de retour
#   - Raises: exceptions possibles
#   - Attributes: attributs d'une classe
#   - Methods: méthodes d'une classe
#   - Examples: exemples d'utilisation (doctest possible)
#   - Notes: informations supplémentaires
#   - Warning: avertissements
#   - See Also: liens vers d’autres fonctions
#   - Yields: si la fonction est un générateur
#   - Deprecated: version obsolète

def ma_fonction(a, b):
    """
    Description brève de la fonction.

    Args:
        a (int): Première valeur.
        b (float): Deuxième valeur.

    Returns:
        float: La somme de a et b.

    Raises:
        ValueError: Si a est négatif.
        TypeError: Si b n'est pas un float.

    Notes:
        Cette fonction est un exemple.

    Examples:
        >>> ma_fonction(2, 3.5)
        5.5
    """
    return a + b

def process_orders(orders, tax_rate=0.2, *, apply_discount=False, logger=None):
    """
    Traite une liste de commandes et calcule leur coût total final.

    Cette fonction parcourt chaque commande, applique les taxes, les remises
    éventuelles, et génère un rapport résumant les montants traités. Elle peut
    également journaliser l’activité si un logger est fourni.

    Args:
        orders (list[dict]): Liste des commandes.
            Chaque commande doit contenir :
                - "id" (int): Identifiant de la commande.
                - "price" (float): Prix de base.
                - "quantity" (int): Quantité.
                - "discount" (float, optional): Remise entre 0 et 1.
        tax_rate (float, optional): Taux de taxe à appliquer (par défaut 0.2).
        apply_discount (bool, optional): Indique si les remises doivent être appliquées avant taxe.
        logger (logging.Logger, optional): Logger pour tracer les opérations exécutées.

    Returns:
        dict: Un dictionnaire contenant :
            - "total_before_tax" (float): Total sans taxes.
            - "total_after_tax" (float): Total final après taxe.
            - "order_count" (int): Nombre de commandes traitées.

    Raises:
        ValueError: Si une commande est mal formée ou si un prix est négatif.
        TypeError: Si 'orders' n'est pas une liste ou si un champ est du mauvais type.

    Warnings:
        Un avertissement est émis si une remise dépasse 50 %.

    Notes:
        - Les remises ne s'appliquent que si `apply_discount=True`.
        - Les valeurs sont calculées avec une précision double (float standard Python).
        - La fonction ne persiste rien en base de données.

    Examples:
        >>> process_orders([
        ...     {"id": 1, "price": 100, "quantity": 2},
        ...     {"id": 2, "price": 50, "quantity": 1, "discount": 0.1},
        ... ])
        {'total_before_tax': 250.0, 'total_after_tax': 300.0, 'order_count': 2}

    See Also:
        calculate_discount: Fonction utilitaire pour calculer une remise.
        validate_order: Vérifie la structure d'une commande.
    """
    pass