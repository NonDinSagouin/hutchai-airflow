[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-24/11/2025-blue)](./)
![Python](https://img.shields.io/pypi/pyversions/apache-airflow)
![Airflow](https://img.shields.io/badge/Airflow-3.1.3%2B-blue?logo=apacheairflow)

# STRUCTURE GÉNÉRALE DU PROJET `hutchai-airflow`
---

### 🔹 Table des matières
1. Présentation générale
1. Arborescence et description des sous-dossiers
1. Commandes Makefile
1. Description des DAGs

### 🔹 Présentation générale
Ce projet met en œuvre des pipelines ETL avec Apache Airflow pour l'orchestration, l'extraction, la transformation, l'insertion et la vérification de données.
Il utilise Apache Spark pour les traitements distribués et les transformations de données à grande échelle.
Il est organisé en plusieurs dossiers pour assurer une séparation claire des responsabilités et une meilleure maintenabilité.

### 🔹 Arborescence et description des sous-dossiers

- 🗂️ app/`
    - 🗂️ `helper/` : Fonctions utilitaires pour la manipulation de fichiers, l'exécution SQL, la gestion de templates, etc <📑>.
    - 🗂️ `classes/` : Classes principales pour l'intégration et l'automatisation avec Airflow <📑>.
    - 🗂️ `library/` : Librairies internes spécifiques au projet.
    - 🗂️ `sql/` : Scripts SQL utilisés dans les pipelines.
    - 🗂️ `static/` : Fichiers statiques (ressources, modèles, etc.) <📑>.
    - 🗂️ `tasks/` : Contient les opérateurs, scripts et groupes de tâches Airflow <📑>.
    - 🗂️ `tests/` : Tests unitaires et d'intégration pour les modules de l'application.
- 🗂️ `config/` : Fichiers de configuration du projet (ex : config.ini).
- 🗂️ `dags/` : Définition des DAGs Airflow orchestrant les workflows ETL.
- 🗂️ `docker/` : Fichiers Docker pour la configuration des services et de l'environnement d'exécution <📑>.
- 🗂️ `keys/` : Clés d'accès et secrets nécessaires à certains traitements. _(Ignoré par Git, Ode)_
- 🗂️ `logs/` : Dossiers de logs générés par Airflow et les pipelines. _(Ignoré par Git, Ode)_
- 🗂️ `plugins/` : Plugins personnalisés pour Airflow. _(Ignoré par Git, Ode)_
- 🗂️ `secure/` : Fichiers sensibles ou sécurisés. _(Ignoré par Git, Ode)_
- 🗂️ `temp/` : Fichiers temporaires générés lors de l'exécution des pipelines. _(Ignoré par Git)_
- 🗂️ `images/` : Dossier images pour la documentation ou les ressources. _(Ignoré par Ode)_
- 🗂️ `docker/` : Fichiers Docker pour la configuration des services. _(Ignoré par Ode)_
- 🗂️ `export/` : Dossier d'export de fichiers. Sur les serveurs, le dossier d'export est synchronisé avec un dossier partagé en SFTP. _(Ignoré par Git, Ode)_
- 🗂️ `tests/` : Dossier de tests globaux du projet.
- 🗂️ `tools/` : Scripts utilitaires pour automatiser des tâches ou interagir avec Airflow <📑>.

* <📑> voir README dédié pour le détail

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

La section `dags/` contient les définitions des DAGs Airflow qui orchestrent les différents workflows ETL du projet. Chaque DAG décrit une suite de tâches automatisées, leur ordre d'exécution, ainsi que les dépendances entre elles. Les DAGs sont conçus pour être modulaires, réutilisables et facilement configurables selon les besoins métiers.

> Pour plus de détails sur chaque dossier, consultez le README spécifique à l'intérieur de chaque sous-dossier.
