import logging

from typing import Dict

def create_merge_sql(
    target_table: str,
    source_table: str,
    key_mapping: Dict[str, str],
    mapping_update: Dict[str, str],
    mapping_insert: Dict[str, str] = None,
    ) -> str:
    """ Crée une requête MERGE SQL Server avec un mapping flexible

    Args:
        target_table (str): Nom de la table cible.
        source_table (str): Nom de la table source.
        key_mapping (Dict[str, str]): Dictionnaire {colonne_cible: colonne_source} pour la clé.
        mapping_update (Dict[str, str]): Dictionnaire {colonne_cible: colonne_source} pour les colonnes à mettre à jour, avec des valeurs supplémentaires si nécessaire.
        mapping_insert (Dict[str, str], optional): Dictionnaire {colonne_cible: colonne_source} pour les colonnes à insérer, avec des valeurs supplémentaires si nécessaire.. Defaults to None.

    Returns:
        str: Requête SQL MERGE sous forme de chaîne.
    """

    # Construction de la condition ON pour la clé primaire
    on_clause = " AND ".join([f"target.{t} = source.{s}" for t, s in key_mapping.items()])

    # Construction de la clause UPDATE
    update_clause = ",\n        ".join([f"target.{t} = source.{s}" for t, s in mapping_update.items()])

    # Colonnes et valeurs d'insertion
    insert_columns = list(key_mapping.keys()) + list(mapping_insert.keys())
    insert_values = [f"source.{s}" for s in list(key_mapping.values()) + list(mapping_insert.values())]

    # Construction de la partie INSERT
    insert_columns_str = ", ".join(insert_columns)
    insert_values_str = ", ".join([f"{v}" if isinstance(v, str) else str(v) for v in insert_values])  # Pas de guillemets inutiles

    # Génération de la requête MERGE
    query = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_str})
            VALUES ({insert_values_str});
    """

    logging.info("✅ Requête MERGE générée !")
    logging.info(query)

    return query
