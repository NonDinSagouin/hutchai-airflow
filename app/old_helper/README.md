[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-26/06/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - dossier app/helper

---

## Changelog
- 26/06/2025 : Uniformisation du README, ajout de la date de mise à jour et d'une section changelog.

---

### Présentation générale
Le dossier `helper` regroupe des fonctions utilitaires facilitant la manipulation de fichiers,
    🔹 l'exécution de requêtes SQL,
    🔹 la gestion de templates Jinja,
    🔹 la génération de fichiers temporaires,
    🔹 l'intégration avec Airflow.

### Fonctions disponibles
    -🔸 apply_jinja(query : str, params : dict) : Applique un template Jinja à une requête SQL.
    -🔸 create_merge_sql(...) : Génère dynamiquement une requête SQL de type MERGE.
    -🔸 generate_csv_to_temp(...) : Génère un fichier CSV temporaire à partir de données.
        📌 Possible de crée dans un sous-répertoire du DAG. exemple : "download/OJS_download_{file}.csv"
    -🔸 generate_json_to_temp(dag_id, data: dict, file_name : str) : Génère un fichier JSON temporaire.
    -🔸 get_sqlserver_dataset(engine: Engine, table: str, schema: str = "dbo") -> Dataset : Récupère un dataset depuis SQL Server.
    -🔸 load_csv_df(...) : Charge un fichier CSV dans un DataFrame.
    -🔸 get_file_path(...) : Récupère le chemin d'un fichier CSV.
    -🔸 load_file_params(dag_id) : Charge les paramètres d'un fichier pour un DAG donné.
    -🔸 load_json_to_dict(dag_id, file_name) : Charge un fichier JSON en dictionnaire.
    -🔸 load_kwargs_informations(add_context: bool = False, **kwargs) -> dict[str, Any] : Récupère les informations des kwargs Airflow.
    -🔸 load_sql_file(file_path: str) : Charge le contenu d'un fichier SQL.
    -🔸 load_tags(dag : DAG) : Charge les tags associés à un DAG.
    -🔸 load_xcom_df(...) : Charge un XCom Airflow dans un DataFrame.
    -🔸 logging_title(text: str, lvl: int = 1) : Affiche un titre formaté dans les logs.
    -🔸 sql_execute(engine : engine, query : str) -> bool : Exécute une requête SQL.
    -🔸 sql_fetch_all(...) : Récupère tous les résultats d'une requête SQL.

Chaque fonction est documentée dans son fichier source pour plus de détails sur les paramètres et l'utilisation.