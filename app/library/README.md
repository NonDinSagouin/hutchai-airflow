[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier app/library`

---

### 🔹Table des matières
1. Présentation générale
1. Arborescence des dossiers et scripts
1. Description détaillée des fichiers

### 🔹Présentation générale
Le dossier `library` contient les bibliothèques de constantes et de définitions partagées du projet. Ces bibliothèques offrent un référentiel centralisé de tags et de constantes utilisés à travers les différentes tâches et DAGs Airflow.

### 🔹Arborescence et description des fichiers

- **TagsLibrary.py** : Bibliothèque de constantes de tags pour le projet. Contient des tags pour les opérations de traitement de données (extraction, insertion, transformation, import, export), les interactions avec des API, et des tags spécifiques aux domaines métiers (jeux vidéo, warehouse, dimensions). Permet une standardisation des tags utilisés dans les DAGs et garantit la cohérence de la nomenclature.

Pour plus d'informations, se référer à la documentation de chaque script.
