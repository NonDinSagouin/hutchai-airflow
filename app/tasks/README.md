[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier app/tasks`

---

### 🔹Table des matières
1. Présentation générale
1. Arborescence des dossiers et scripts
1. Description détaillée des fichiers

### 🔹Présentation générale
Le dossier `tasks` contient les tâches (opérateurs) personnalisées du projet. Ces classes définissent les différentes opérations réutilisables qui peuvent être utilisées dans les DAGs Airflow pour interagir avec des API, transformer des données, gérer des bases de données et exécuter des opérations diverses.

### 🔹Arborescence et description des fichiers

#### Fichier racine
- **decorateurs.py** : Définit des décorateurs personnalisés pour les tâches Airflow. Le décorateur `customTask` ajoute automatiquement des logs de début et fin d'exécution avec des emojis pour améliorer la lisibilité des logs et standardiser le comportement des tâches.

#### Dossier `api/`
Contient les tâches pour interagir avec des APIs externes.

- **Providers.py** : Tâches génériques pour appeler des APIs de providers.
- **Riotgames.py** : Tâches spécialisées pour interagir avec l'API Riot Games (League of Legends). Permet de récupérer des données de matchs, de joueurs et statistiques avec gestion des rate limits et des tokens d'authentification.

#### Dossier `autres/`
Contient des tâches utilitaires diverses.

- **Timer.py** : Tâche pour créer des pauses temporelles dans l'exécution d'un DAG. Utile pour attendre entre des appels API ou respecter des contraintes temporelles.

#### Dossier `databases/`
Contient les tâches pour interagir avec les bases de données.

- **PostgresWarehouse.py** : Tâches pour gérer les opérations sur PostgreSQL (warehouse). Permet de créer des schémas, tables, insérer des données en masse, effectuer des merges et gérer la structure des données dans le data warehouse.

#### Dossier `transformation/`
Contient les tâches de transformation de données.

- **AddColumns.py** : Tâche pour ajouter des colonnes techniques aux DataFrames (tech_dag_id, tech_execution_date, etc.). Facilite le traçage et l'audit des données transformées.
- **Clean.py** : Tâches de nettoyage de données (suppression de doublons, gestion des valeurs nulles, normalisation).
- **Combine.py** : Tâches pour combiner plusieurs DataFrames (merge, concat, join).

#### Dossier `z_exemple/`
Contient des exemples de tâches pour servir de templates et de références lors du développement de nouvelles tâches.

Pour plus d'informations, se référer à la documentation de chaque script.
