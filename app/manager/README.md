[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier app/manager`

---

### 🔹Table des matières
1. Présentation générale
1. Arborescence des dossiers et scripts
1. Description détaillée des fichiers

### 🔹Présentation générale
Le dossier `manager` contient les gestionnaires de ressources et de connexions du projet. Ces classes offrent une interface centralisée pour gérer les connexions aux différents services (bases de données, Spark, XCom) avec des mécanismes de cache et d'optimisation.

### 🔹Arborescence et description des fichiers

- **Connectors.py** : Gestionnaire centralisé de connexions aux différentes sources de données. Fournit des méthodes statiques pour obtenir des connexions HTTP, SQL et autres avec un système de cache pour optimiser les performances et éviter les connexions multiples inutiles. Gère automatiquement les erreurs de connexion.
- **Spark.py** : Gestionnaire de sessions Spark pour Airflow. Permet de créer et réutiliser des SparkSession avec des configurations personnalisables (mémoire, partitions, timeouts). Offre une interface simplifiée pour l'intégration de Spark dans les tâches Airflow avec gestion optimisée des ressources.
- **Xcom.py** : Gestionnaire de données XCom pour le partage d'informations entre tâches Airflow. Permet de récupérer et de stocker des données (DataFrames, dictionnaires, chaînes) via XCom.

Pour plus d'informations, se référer à la documentation de chaque script.
