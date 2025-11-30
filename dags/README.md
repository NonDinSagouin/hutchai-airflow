[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-18/08/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier dags`


---

## Changelog
- 18/08/2025 : Ajout de la documentation des 5 nouveaux DAGs GDP_XXX.
- 01/08/2025 : Création du README pour le dossier dags.

---

### 🔹Table des matières
1. Présentation générale
1. Description des DAGs disponibles
1. Organisation et conventions de nommage

### 🔹Présentation générale
Le dossier `dags` contient les définitions des DAGs (Directed Acyclic Graphs) d'Apache Airflow pour le projet A2PO. Ces DAGs orchestrent l'ensemble des pipelines de données, depuis l'extraction jusqu'à l'insertion, en passant par les transformations et vérifications.

### 🔹Description des DAGs disponibles

- 📥 **Entrants_etudes_import.py**
Extraction et transformation des données des entrants études.
_Ce DAG utilise les données d'étude PVDR depuis GCP pour alimenter les tables d'entrants avec les informations nécessaires aux analyses d'études._

- 🚀 **Entrants_etudes_mesure.py**
Mesures et analyses des données d'entrants études.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données d'entrants études préalablement importées._

- 🚀 **Export_referentiel_a2po.py**
Export des référentiels A2PO vers Google Cloud Platform.
_Ce DAG exporte les référentiels d'A2PO et Noisette dans Google Cloud Storage (GCS) puis les charge dans BigQuery. Exécution quotidienne à 02h00._

- 🛠️ **Exemples.py**
DAG d'exemple et de démonstration.
_DAG exemple pour illustrer l'extraction de données depuis SQL Server d'A2PO et BigQuery de GCP. Utilisé pour les tests et la formation._

- 📥 **GDP_etudes_import.py**
Import des données des études GDP depuis OJS.
_Ce DAG permet d'importer les données des études depuis OJS vers la table warehouse.GDP_ETUDES. Exécution tous les Dimanche à 18h00._

- 🚀 **GDP_etudes_planifie_mesure.py**
Mesures et analyses des données du planifié des études GDP.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données du planifié des études en utilisant la table warehouse.GDP_ETUDES. S'exécute à la suite de GDP_etudes_import._

- 🚀 **GDP_etudes_realise_mesure.py**
Mesures et analyses des données du réalisé des études GDP.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données du réalisé des études en utilisant la table warehouse.GDP_ETUDES. S'exécute à la suite de GDP_etudes_import._

- 🚀 **GDP_etudes_stock_aff_mesure.py**
Mesures et analyses des données du stock affecté des études GDP.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données du stock affecté des études en utilisant la table warehouse.GDP_ETUDES. S'exécute à la suite de GDP_etudes_import._

- 🚀 **GDP_etudes_stock_exp_mesure.py**
Mesures et analyses des données du stock exploitable des études GDP.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données du stock exploitable des études en utilisant la table warehouse.GDP_ETUDES. S'exécute à la suite de GDP_etudes_import._

-🔌 **GPA_deploy_files.py**
Déploiement des fichiers GPA sur le serveur A2PO.
_Ce DAG permet de déployer les fichiers GPA_indispo et GPA_occupation sur le serveur A2PO suite aux imports correspondants._

- 📥 **GPA_indispo_import.py**
Import des données d'indisponibilité GPA.
_Pipeline d'importation et de traitement des données d'indisponibilité depuis les sources GPA vers les systèmes A2PO._

- 📥 **GPA_occupation_import.py**
Import des données d'occupation GPA.
_Pipeline d'importation et de traitement des données d'occupation depuis les sources GPA vers les systèmes A2PO._

- 📥 **ML_FTTH_import.py**
Import des données de prévision FTTH par Machine Learning.
_Ce DAG importe les données de prévision FTTH depuis GCP et les insère dans la base de données A2PO. Exécution mensuelle le 7 de chaque mois à 01h00._

- 📥 **Realise_contrast_import.py**
Import des données de réalisé contrasté.
_Pipeline d'importation des données de réalisé contrasté pour les analyses de performance et de comparaison._

- 🚀 **Realise_contrast_mesure.py**
Mesures et analyses des données de réalisé contrasté.
_Pipeline de traitement pour effectuer les mesures et calculs sur les données de réalisé contrasté préalablement importées._

### 🔹Organisation et conventions de nommage

#### Structure des noms de fichiers :
- **[Domaine]_[Action]_[Type].py** : Convention principale
  - `Domaine` : Source ou contexte métier (GPA, ML_FTTH, Entrants_etudes, etc.)
  - `Action` : Type d'opération (import, export, deploy, mesure)

#### Catégories de DAGs :
- **Import** : DAGs d'importation de données depuis différentes sources
- **Export** : DAGs d'exportation vers des systèmes externes
- **Mesure** : DAGs de calcul et d'analyse des données
- **Deploy** : DAGs de déploiement de fichiers ou configurations
- **Exemple** : DAGs de démonstration et d'apprentissage

#### Planification commune :
- **Quotidien** : Export_referentiel_a2po (02h00)
- **Hebdomadaire** : GDP_etudes_import (Dimanche à 18h00)
- **Mensuel** : ML_FTTH_import (7 de chaque mois à 01h00)
- **Sur déclenchement** : La plupart des DAGs d'import et de mesure, GDP_etudes_XXX_mesure (suite à GDP_etudes_import)
- **Manuel/Test** : Exemples

Pour plus d'informations sur chaque DAG, consulter les commentaires et la documentation intégrée dans chaque fichier.
