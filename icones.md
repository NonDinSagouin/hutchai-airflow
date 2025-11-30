[![Mise à jour](https://img.shields.io/badge/dernière%20mise%20à%20jour-05/08/2025-blue)](./)

# 🎨 Guide des Icônes - Projet Susanoo-airflow

Ce fichier documente toutes les icônes utilisées dans les Makefiles et leur signification.

## 📋 Guide des Émojis pour Logs et Messages

### Statut et Validation

| Icône | Description | Usage |
|-------|-------------|-------|
| ✅ | **Succès** | Opérations réussies |
| ❌ | **Échec** | Erreurs et échecs |
| ⚠️ | **Avertissement** | Messages d'attention |
| ⛔️ | **Accès refusé** | Problèmes d'autorisation |
| ℹ️ | **Information** | Messages informatifs |
| 🔵 | **Processus en cours** | Opérations actives |
| ⏳ | **Attente** | Processus en attente |
| 🚀 | **Exécution en cours** | Lancement d'opérations |

### Marques et Points de Repère

| Icône | Description | Usage |
|-------|-------------|-------|
| 📌 | **Point clé** | Éléments importants |
| 🔹 | **Étape intermédiaire** | Étapes de processus |
| 🔸 | **Étape importante** | Étapes critiques |
| 🔺 | **Attention** | Points nécessitant attention |
| 🔻 | **Réduction ou suppression** | Opérations de nettoyage |

### Analyse et Données

| Icône | Description | Usage |
|-------|-------------|-------|
| 📊 | **Statistiques** | Données et métriques |
| 📈 | **Augmentation** | Croissance des données |
| 📉 | **Diminution** | Réduction des données |
| 🏷️ | **Catégorisation** | Classification |
| 📑 | **Fichier ou document** | Gestion de fichiers |
| 🗂️ | **Dossier** | Organisation des dossiers |

### Outils et Processus

| Icône | Description | Usage |
|-------|-------------|-------|
| 🔧 | **Configuration** | Paramétrage système |
| 🏗️ | **Construction** | Processus de build |
| ⚙️ | **Processus technique** | Opérations techniques |
| 🖥️ | **Exécution informatique** | Traitement système |
| 📝 | **Écriture de fichier** | Création de fichiers |
| 🔌 | **Connexion** | Établissement de liens |
| 💾 | **Sauvegarde** | Opérations de backup |
| 📥 | **Insert** | Insertion de données |

### Processus et Boucles

| Icône | Description | Usage |
|-------|-------------|-------|
| 🔄 | **Rafraîchissement ou mise à jour** | Actualisation |
| 🔁 | **Répétition** | Processus itératifs |
| ⏩ | **Avancement rapide** | Accélération |
| ⏪ | **Retour en arrière** | Rollback |

### Performance et Actions

| Icône | Description | Usage |
|-------|-------------|-------|
| 🚀 | **Action rapide** | Opérations rapides |
| 🔥 | **Processus intense** | Traitement lourd |
| ⚡ | **Exécution instantanée** | Actions immédiates |

## 💡 Exemples d'Utilisation dans les Logs

### Messages de Log Python
```python
logging.info("🔹 Vérification des données en cours...")
logging.info("✅ Succès : 2000 lignes insérées.")
logging.warning("⚠️ Attention : certaines données sont manquantes.")
logging.debug("🚀 Lancement du traitement final...")
```

### Messages de DAG Airflow
```python
# Pour les tâches de récupération
logging.info("📥 Récupération des fichiers ...")

# Pour les transformations
logging.info("🔧 Transformation des données en cours...")

# Pour les jointures
logging.info("🔌 Jointure avec les tables de référence...")

# Pour l'injection finale
logging.info("💾 Injection des données dans [table]...")
```

## ⏰ Référence CRON

### Notation CRON
Format : `mm hh jj MMM JJJ`

- **mm** : minutes (0 à 59)
- **hh** : heure (0 à 23)
- **jj** : jour du mois (1 à 31)
- **MMM** : mois (jan, feb, ... ou 1 à 12)
- **JJJ** : jour de la semaine :
  - 0 = dimanche
  - 1 = lundi
  - 2 = mardi
  - 3 = mercredi
  - 4 = jeudi
  - 5 = vendredi
  - 6 = samedi

### Exemples CRON pour Airflow
```python
# Tous les dimanche à 18h (comme GDP_etudes_import)
schedule="0 18 * * 0"

# Tous les jours à minuit
schedule="0 0 * * *"

# Toutes les heures
schedule="0 * * * *"

# Tous les lundi à 8h30
schedule="30 8 * * 1"
```
