PROJECT_NAME=hutchai-airflow

# Charger les variables d'environnement depuis .env
include .env
export

# Variables
DC=docker compose
COMPOSE_FILE = ./docker-compose.yml
backupKey=K2Hf9UR96wmcQRwZ5CsJwPlQc

# Nom du service Airflow dans Docker Compose
SERVICE=$(PROJECT_NAME)-airflow-apiserver-1
SCHEDULER=$(PROJECT_NAME)-airflow-scheduler-1
TRIGGERER=$(PROJECT_NAME)-airflow-triggerer-1
WORKER=$(PROJECT_NAME)-airflow-worker-1
WAREHOUSE=$(PROJECT_NAME)-warehouse-1

FILE_SECURE=./secure
FILE_KEYS=./keys

# Fichiers de sortie pour les exportations
JSON_VARIABLES_FILE=variables.json
JSON_CONNECTIONS_FILE=connections.json
JSON_POOLS_FILE=pools.json
JSON_KEYS_FILE=keys.json

DE = docker exec -it
DOCKER_EXEC = $(DE) $(SERVICE) /bin/bash -c

# Variables pour la configuration du DNS
NAMESERVERS = "nameserver 10.114.20.134" "nameserver 10.114.72.134"
RESOLV_CONF = /etc/resolv.conf

.PHONY: help

# ====================================
# Commande principale
# ====================================
# Aide pour afficher les commandes disponibles
help: ## Affiche cette aide
	@echo "======================================"
	@echo "📋 Commandes disponibles"
	@echo "======================================"
	@echo ""
	@echo "🐳 Docker (make/docker.mk)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' make/docker.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "✈️  Airflow (make/airflow.mk)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' make/airflow.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "💾 Database (make/database.mk)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' make/database.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "⚡ Spark (make/spark.mk)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' make/spark.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "🔍 Vérification (make/check.mk)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' make/check.mk | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ====================================
# Inclure les fichiers de commandes modulaires
# ====================================
include make/docker.mk
include make/airflow.mk
include make/database.mk
include make/spark.mk
include make/check.mk


