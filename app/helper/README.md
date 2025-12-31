[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier app/helper`

---

### 🔹Table des matières
1. Présentation générale
1. Arborescence des dossiers et scripts
1. Description détaillée des fichiers

### 🔹Présentation générale
Le dossier `helper` contient les fonctions utilitaires et helpers du projet. Ces fonctions offrent des fonctionnalités transversales utilisées à travers les différentes tâches Airflow.

### 🔹Arborescence et description des fichiers

- **call_api.py** : Fonction utilitaire pour effectuer des appels API REST avec gestion des erreurs. Permet d'appeler des endpoints externes et de récupérer des données JSON avec une gestion automatique des erreurs HTTP.
- **logging_title.py** : Fonction pour créer des titres formatés dans les logs avec différents niveaux de hiérarchie (1-5). Facilite la lisibilité des logs Airflow en structurant visuellement les différentes étapes d'exécution.

Pour plus d'informations, se référer à la documentation de chaque script.
