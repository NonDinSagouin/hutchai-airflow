[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)
![Airflow](https://img.shields.io/badge/Airflow-3.1.3%2B-blue?logo=apacheairflow)

# STRUCTURE GÉNÉRALE DU PROJET `hutchai-airflow`
---

### 🔹 Table des matières
1. Présentation générale
1. Arborescence et description des sous-dossiers
1. Commandes Makefile
1. Description des DAGs

### 🔹 Présentation générale
Ce projet met en œuvre des pipelines ETL avec Apache Airflow pour l'orchestration de workflows de données League of Legends. Il extrait, transforme et charge des données depuis l'API Riot Games vers un data warehouse PostgreSQL. Le projet utilise Apache Spark pour les traitements distribués et est organisé en plusieurs dossiers pour assurer une séparation claire des responsabilités et une meilleure maintenabilité.

### 🔹 Arborescence et description des sous-dossiers

- 🗂️ `app/` : Code applicatif principal du projet
    - 🗂️ `helper/` : Fonctions utilitaires pour les appels API, le logging et la manipulation de données [📑](app/helper/README.md).
    - 🗂️ `library/` : Bibliothèques de constantes et tags partagés (TagsLibrary) [📑](app/library/README.md).
    - 🗂️ `manager/` : Gestionnaires de ressources (Connectors, Spark, Xcom) pour les connexions et sessions [📑](app/manager/README.md).
    - 🗂️ `old_helper/` : Anciennes fonctions utilitaires conservées pour référence.
    - 🗂️ `old_tasks/` : Anciennes tâches conservées pour référence.
    - 🗂️ `sql/` : Scripts SQL utilisés dans les pipelines.
    - 🗂️ `tasks/` : Tâches Airflow personnalisées organisées par catégorie (api, databases, transformation, autres) [📑](app/tasks/README.md).
    - 🗂️ `tests/` : Tests unitaires et d'intégration pour les modules de l'application.
- 🗂️ `config/` : Fichiers de configuration du projet (airflow.cfg, config.ini).
- 🗂️ `dags/` : Définition des DAGs Airflow orchestrant les workflows ETL pour League of Legends [📑](dags/README.md).
- 🗂️ `docker/` : Configuration Docker Compose et Dockerfiles pour Airflow, Spark et le warehouse.
- 🗂️ `export/` : Dossier d'export de fichiers. _(Ignoré par Git)_
- 🗂️ `images/` : Ressources visuelles pour la documentation.
- 🗂️ `keys/` : Clés d'accès et secrets nécessaires aux traitements. _(Ignoré par Git)_
- 🗂️ `logs/` : Logs générés par Airflow et les pipelines. _(Ignoré par Git)_
- 🗂️ `make/` : Fichiers Makefile organisés par thématique (airflow.mk, docker.mk, database.mk, spark.mk, check.mk).
- 🗂️ `plugins/` : Plugins personnalisés pour Airflow. _(Ignoré par Git)_
- 🗂️ `secure/` : Fichiers de configuration sensibles (connections.json, variables.json, pools.json). _(Ignoré par Git)_
- 🗂️ `temp/` : Fichiers temporaires générés lors de l'exécution. _(Ignoré par Git)_
- 🗂️ `tools/` : Scripts utilitaires pour automatiser des tâches ou interagir avec Airflow (api_run_dag.py, clear_dag_runs.py).

> [📑] voir README dédié pour plus de détails

### 🔹 Commandes Makefile

Le projet inclut un Makefile avec plusieurs commandes utilitaires pour simplifier la gestion du projet Airflow :

#### 📋 Commandes de base
- `make help` : Affiche l'aide avec toutes les commandes disponibles

#### 🐳 Gestion des services Docker
- `make up` : Lancer les services avec Docker Compose
- `make down` : Arrêter et supprimer les services
- `make restart` : Arrêter puis relancer les services
- `make init` : Initialiser la base de données Airflow et démarrer les services

#### 📦 Import/Export de configuration
- `make export` : Exporter les variables, connexions et pools Airflow
- `make import` : Importer les variables, connexions et pools Airflow

#### 🔧 Maintenance et développement
- `make tests` : Lancer les tests unitaires
- `make clear_dags_run` : Nettoyer les exécutions des DAGs (interactif)
- `make reload_dags` : Recharger les DAGs dans Airflow
- `make reset_all` : Réinitialisation complète de la base de données Airflow et suppression de toutes les exécutions (interactif)

#### 🔍 Vérification et diagnostic
- `make check` : Vérification complète de l'état des services, connexions et configurations
- `make check-docker` : Vérifier et démarrer le démon Docker et wsl-vpnkit si nécessaire
- `make check-container` : Vérifier l'état des services Airflow et attendre qu'ils soient healthy

> **💡 Conseil** : Utilisez `make help` pour afficher la liste complète des commandes avec leurs descriptions.

### 🔹 Description des DAGs

Le dossier `dags/` contient les définitions des DAGs Airflow qui orchestrent les différents workflows ETL du projet League of Legends. Le projet se concentre sur l'extraction, la transformation et le chargement de données depuis l'API Riot Games vers un data warehouse PostgreSQL.

#### DAGs de production
- **LOL_referentiel** : Gestion des données de référence des champions
- **LOL_enrich_fact_puuid** : Enrichissement des informations de joueurs (PUUIDs)
- **LOL_enrich_fact_matchs** : Récupération des identifiants de matchs
- **LOL_enrich_fact_stats** : Extraction et stockage des statistiques détaillées des matchs

Pour plus de détails, consultez le [README des DAGs](dags/README.md).

### 🔹 Architecture technique

Le projet utilise une stack complète pour le traitement de données :
- **Apache Airflow 3.1.3+** : Orchestration des workflows ETL
- **Apache Spark** : Traitement distribué des données (configuré mais non utilisé dans les DAGs actuels)
- **PostgreSQL** : Data warehouse pour le stockage des données League of Legends
- **Docker & Docker Compose** : Containerisation des services (Airflow, Spark, Warehouse)
- **Python 3.x** : Langage principal pour les tâches et transformations

> Pour plus de détails sur chaque dossier, consultez le README spécifique à l'intérieur de chaque sous-dossier.
