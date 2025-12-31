[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-31/12/2025-blue)](./)

# STRUCTURE DU PROJET AIRFLOW - `dossier dags`

---

### 🔹Table des matières
1. Présentation générale
1. Description des DAGs disponibles
1. Organisation et conventions de nommage

### 🔹Présentation générale
Le dossier `dags` contient les définitions des DAGs (Directed Acyclic Graphs) d'Apache Airflow pour le projet. Ces DAGs orchestrent l'ensemble des pipelines de données League of Legends, depuis l'extraction des données via l'API Riot Games jusqu'à l'insertion dans le data warehouse, en passant par les transformations et enrichissements.

### 🔹Description des DAGs disponibles

#### DAGs de production League of Legends

- 🎮 **LOL_referentiel.py**
Extraction, transformation et chargement des données de référence des champions League of Legends depuis l'API DDragon vers l'entrepôt de données. Ce DAG permet de maintenir à jour le référentiel des champions avec leurs caractéristiques (nom, ID, titre, description, tags).
_Planification : Manuel_

- 👤 **LOL_enrich_fact_puuid.py**
Récupération et enrichissement des informations de PUUID (identifiants de joueurs) League of Legends via l'API Riot Games. Extrait les PUUIDs depuis la table factuelle, interroge l'API pour obtenir des informations détaillées (game_name, tag_line), et met à jour la table factuelle.
_Planification : Toutes les 3 minutes entre 19h et 23h_

- 🎯 **LOL_enrich_fact_matchs.py**
Récupération des identifiants de matchs des joueurs League of Legends. Extrait les PUUIDs depuis la table factuelle, interroge l'API Riot Games pour obtenir les identifiants des matchs associés, stocke ces données dans les tables brutes puis factuelles de l'entrepôt.
_Planification : Quotidien à 09h00_

- 📊 **LOL_enrich_fact_stats.py**
Récupération et stockage des statistiques détaillées des matchs League of Legends. Extrait les identifiants de matchs, interroge l'API Riot Games pour obtenir les détails complets, et extrait les statistiques des participants pour les stocker dans une table factuelle dédiée.
_Planification : Toutes les 3 minutes entre 10h et 18h_

#### DAGs de test et exemples

- 📝 **z_Exemples.py**
DAG de démonstration contenant des exemples d'utilisation des différentes tâches et patterns disponibles dans le projet. Sert de référence pour le développement de nouveaux DAGs.

- ⚡ **z_Stress_test.py**
DAG de tests de performance et de charge pour vérifier la robustesse du système sous contrainte.

- 🗂️ **z_vide.py**
DAG template vide servant de base pour la création de nouveaux DAGs.

### 🔹Organisation et conventions de nommage

#### Structure des noms de fichiers :
- **[Domaine]_[Action]_[Ressource].py** : Convention principale
  - `Domaine` : Contexte métier ou projet (LOL pour League of Legends)
  - `Action` : Type d'opération (referentiel, enrich, extract, transform, etc.)
  - `Ressource` : Type de données traitées (fact_puuid, fact_matchs, fact_stats)

#### Catégories de DAGs :
- **Referentiel** : DAGs de gestion des données de référence
- **Enrich** : DAGs d'enrichissement de données factuelles depuis des APIs
- **Exemples/Tests** : DAGs préfixés par `z_` pour la démonstration et les tests

#### Planification commune :
- **Quotidien** : LOL_enrich_fact_matchs (09h00)
- **Haute fréquence** : LOL_enrich_fact_puuid (*/3 19-23), LOL_enrich_fact_stats (*/3 10-18)
- **Manuel/Test** : LOL_referentiel, z_Exemples, z_Stress_test, z_vide

#### Tags utilisés :
- `league_of_legends` : DAGs relatifs à League of Legends
- `riot_games` : DAGs utilisant l'API Riot Games
- `warehouse` : DAGs interagissant avec le data warehouse
- `data_rows` : DAGs gérant les données brutes
- `data_fact` : DAGs gérant les données factuelles

Pour plus d'informations sur chaque DAG, consulter les commentaires et la documentation intégrée dans chaque fichier.
