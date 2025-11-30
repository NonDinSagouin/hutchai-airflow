# Variables
DC=docker compose
COMPOSE_FILE = ./docker-compose.yml
backupKey=K2Hf9UR96wmcQRwZ5CsJwPlQc

# Nom du service Airflow dans Docker Compose
SERVICE=airflow-airflow-apiserver-1
SCHEDULER=airflow-airflow-scheduler-1
TRIGGERER=airflow-airflow-triggerer-1
WORKER=airflow-airflow-worker-1

FILE_SECURE=./secure
FILE_KEYS=./keys

# Fichiers de sortie pour les exportations
JSON_VARIABLES_FILE=variables.json
JSON_CONNECTIONS_FILE=connections.json
JSON_POOLS_FILE=pools.json
JSON_KEYS_FILE=keys.json

DE = docker exec -it
DOCKER_EXEC = $(DE) $(SERVICE) /bin/bash -c

.PHONY: help up down export import

# Variables pour la configuration du DNS
NAMESERVERS = "nameserver 10.114.20.134" "nameserver 10.114.72.134"
RESOLV_CONF = /etc/resolv.conf

# Aide pour afficher les commandes disponibles
help: ## Affiche cette aide
	@echo "Commandes disponibles :"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

init: ## Génère le fichier .env puis initialise la base de données Airflow
	$(DC) up airflow-init
	$(DC) -f $(COMPOSE_FILE) up -d --build
	sudo chmod 777 temp

build: ## Construire les images Docker personnalisées
	$(DC) -f $(COMPOSE_FILE) up -d --build

up: ## Lancer les services avec Docker Compose
	$(DC) -f $(COMPOSE_FILE) up -d

down: ## Arrêter et supprimer les services avec Docker Compose
	$(DC) -f $(COMPOSE_FILE) down

restart: ## Arrêter puis lancer les services avec Docker Compose
	make down
	make up


export: ## Exporter les variables et les connexions
	@echo "Starting export of Airflow configurations..."
	@if [ -z "$(DOCKER_EXEC)" ]; then echo "DOCKER_EXEC is not set. Aborting."; exit 1; fi
	@if [ -z "$(backupKey)" ]; then echo "backupKey is not set. Aborting."; exit 1; fi

	$(DOCKER_EXEC) "airflow variables export /tmp/$(JSON_VARIABLES_FILE) && \
					airflow connections export /tmp/$(JSON_CONNECTIONS_FILE) && \
					airflow pools export /tmp/$(JSON_POOLS_FILE)"
	@echo "Export successful. Copying files from container..."

	sudo docker cp $(SERVICE):/tmp/$(JSON_VARIABLES_FILE) ./secure/
	sudo docker cp $(SERVICE):/tmp/$(JSON_CONNECTIONS_FILE) ./secure/
	sudo docker cp $(SERVICE):/tmp/$(JSON_POOLS_FILE) ./secure/
	sudo docker cp $(SERVICE):/tmp/$(JSON_KEYS_FILE) ./keys/
	@echo "Files copied: $(FILE_SECURE)/$(JSON_VARIABLES_FILE) and $(FILE_SECURE)/$(JSON_CONNECTIONS_FILE)"

import: ## Importer les variables et les connexions
	@echo "Importing Airflow configurations..."
	@docker cp $(FILE_SECURE)/$(JSON_VARIABLES_FILE) $(SERVICE):/tmp/$(JSON_VARIABLES_FILE) || { echo "Failed to copy $(JSON_VARIABLES_FILE) to container"; exit 1; }
	@docker cp $(FILE_SECURE)/$(JSON_CONNECTIONS_FILE) $(SERVICE):/tmp/$(JSON_CONNECTIONS_FILE) || { echo "Failed to copy $(JSON_CONNECTIONS_FILE) to container"; exit 1; }
	@docker cp $(FILE_SECURE)/$(JSON_POOLS_FILE) $(SERVICE):/tmp/$(JSON_POOLS_FILE) || { echo "Failed to copy $(JSON_POOLS_FILE) to container"; exit 1; }

	@$(DOCKER_EXEC) "airflow variables import /tmp/$(JSON_VARIABLES_FILE) && \
					airflow connections import /tmp/$(JSON_CONNECTIONS_FILE) && \
					airflow pools import /tmp/$(JSON_POOLS_FILE)" || { echo "Failed to import Airflow configurations"; exit 1; }
	@echo "Import completed."

tests: ## Lance les tests unitaires
	@$(DE) $(SERVICE) python -m unittest discover /opt/airflow/app/tests/

clear_dags_run: ## Nettoyer les exécutions des DAGs et leurs logs
	@echo "Cette commande va supprimer toutes les exécutions d'un DAG spécifique et leurs logs associés."
	@echo "Veuillez vous assurer que vous avez les permissions nécessaires pour supprimer les exécutions de DAG et leurs logs."
	@echo "Vous serez invité à saisir le nom du DAG pour lequel vous souhaitez supprimer les exécutions et logs."
	@echo "Cette action est irréversible, veuillez donc procéder avec précaution."
	@echo "Tapez 'all' pour supprimer toutes les exécutions de tous les DAGs et tous leurs logs. \n"

	@read -p "Nom du DAG (ou all) : " dag; \
	echo "Suppression des exécutions de DAG en cours..."; \
	$(DE) $(SERVICE) python tools/clear_dag_runs.py $$dag; \
	echo "Suppression des logs associés en cours..."; \
	if [ "$$dag" = "all" ]; then \
		echo "Suppression de tous les logs des DAGs..."; \
		$(DE) $(SERVICE) bash -c "find /opt/airflow/logs -name '*.log' -type f -delete 2>/dev/null || true"; \
		$(DE) $(SERVICE) bash -c "find /opt/airflow/logs -mindepth 1 -type d -empty -delete 2>/dev/null || true"; \
	else \
		echo "Suppression des logs pour le DAG: $$dag"; \
		$(DE) $(SERVICE) bash -c "find /opt/airflow/logs -path \"*/$$dag/*\" -type f -delete 2>/dev/null || true"; \
		$(DE) $(SERVICE) bash -c "find /opt/airflow/logs -path \"*/$$dag\" -type d -empty -delete 2>/dev/null || true"; \
	fi; \
	echo "Nettoyage des logs terminé."

	@echo "Nettoyage des exécutions de DAGs et de leurs logs terminé."

reload_dags: ## Recharger les DAGs dans Airflow
	@echo "Nettoyage des fichiers .pyc dans le répertoire des DAGs..."
	docker compose exec airflow-scheduler bash -c "find /opt/airflow/dags -name '*.pyc' -delete && airflow dags list"
	@echo "Redémarrage du scheduler Airflow..."
	@docker compose restart airflow-scheduler
	@echo "DAGs rechargés avec succès."

reset_all: ## Reset complet d'Airflow : arrêt des conteneurs, suppression des données et logs, puis relance
	@echo "Cette commande va effectuer un reset complet d'Airflow :"
	@echo "- Arrêt de tous les conteneurs Airflow"
	@echo "- Suppression de la base de données (volumes Docker)"
	@echo "- Suppression de tous les logs"
	@echo "- Suppression des données temporaires"
	@echo "- Relance complète des services"
	@echo "⚠️ Cette action est irréversible, veuillez donc procéder avec précaution."
	@read -p "Êtes-vous sûr de vouloir continuer ? (y/n) : " confirm; \
	if [ "$$confirm" = "y" ]; then \
		echo "🛑 Arrêt de tous les conteneurs Airflow..."; \
		$(DC) -f $(COMPOSE_FILE) down -v --remove-orphans; \
		echo "🔹 Suppression des volumes Docker (base de données)..."; \
		docker volume prune -f; \
		echo "🔹 Suppression de tous les logs..."; \
		sudo rm -rf logs/*; \
		echo "🔹 Suppression des données temporaires..."; \
		sudo rm -rf temp/*; \
		echo "🔹 Réinitialisation complète d'Airflow..."; \
		$(MAKE) init; \
		echo "🔹 Import des configurations..."; \
		$(MAKE) import; \
		echo "✅ Reset complet terminé avec succès !"; \
	else \
		echo "❌ Reset annulé."; \
	fi

check: check-docker check-container ## Vérifier l'état des services Airflow
	@echo "🔍 Vérification terminée."
check-docker: ## Vérifier et (si nécessaire) démarrer le démon Docker et wsl-vpnkit
	@echo "🔍 Vérification du démon Docker..."
	@if docker info >/dev/null 2>&1; then \
		echo "✅ Docker est déjà actif."; \
	else \
		echo "⏳ Démarrage des services nécessaires (wsl-vpnkit, docker)..."; \
		sudo service wsl-vpnkit start >/dev/null 2>&1 || true; \
		sudo service docker start >/dev/null 2>&1 || true; \
		echo "⏳ Attente du démarrage de Docker..."; \
		for i in $$(seq 1 20); do \
			if docker info >/dev/null 2>&1; then \
				echo "✅ Docker démarré."; \
				break; \
			fi; \
			sleep 1; \
		done; \
		if ! docker info >/dev/null 2>&1; then \
			echo "❌ Docker ne s'est pas lancé correctement."; \
			exit 1; \
		fi; \
	fi
check-container: ## Vérifier l'état des services Airflow et attendre qu'ils soient healthy
	@echo "🔍 Vérification de l'état des services Airflow..."
	@services="$(SERVICE) $(SCHEDULER) $(TRIGGERER) $(WORKER)"; \
	all_healthy=false; \
	max_wait=300; \
	elapsed=0; \
	while [ "$$all_healthy" = "false" ] && [ $$elapsed -lt $$max_wait ]; do \
		all_healthy=true; \
		for service in $$services; do \
			status=$$(docker ps --filter "name=$$service" --format "{{.Status}}" 2>/dev/null || echo "not_found"); \
			if [ "$$status" = "not_found" ] || [ "$$status" = "" ]; then \
				echo "⚠️  $$service n'est pas en cours d'exécution"; \
				all_healthy=false; \
			elif echo "$$status" | grep -q "healthy"; then \
				echo "✅ $$service est healthy"; \
			elif echo "$$status" | grep -q "starting\|unhealthy"; then \
				echo "⏳ $$service est en cours de démarrage ou unhealthy ($$status)"; \
				all_healthy=false; \
			else \
				echo "🔍 $$service status: $$status"; \
				all_healthy=false; \
			fi; \
		done; \
		if [ "$$all_healthy" = "false" ]; then \
			echo "⏳ Attente de 10 secondes avant nouvelle vérification..."; \
			sleep 10; \
			elapsed=$$((elapsed + 10)); \
		fi; \
	done; \
	if [ "$$all_healthy" = "true" ]; then \
		echo "✅ Tous les services Airflow sont healthy !"; \
	else \
		echo "❌ Timeout atteint ($$max_wait secondes) - certains services ne sont pas healthy"; \
		exit 1; \
	fi

# Commandes Spark
spark-ui: ## Ouvre l'interface Spark Master dans le navigateur
	@echo "🌐 Ouverture de l'interface Spark Master..."
	@xdg-open http://localhost:8090 2>/dev/null || open http://localhost:8090 2>/dev/null || echo "Interface disponible sur http://localhost:8090"

spark-logs: ## Affiche les logs du cluster Spark
	@echo "📋 Logs Spark Master:"
	@$(DC) logs --tail=50 spark-master
	@echo "\n📋 Logs Spark Worker 1:"
	@$(DC) logs --tail=30 spark-worker-1
	@echo "\n📋 Logs Spark Worker 2:"
	@$(DC) logs --tail=30 spark-worker-2

spark-status: ## Vérifie le statut du cluster Spark
	@echo "🔍 Statut du cluster Spark:"
	@$(DC) ps | grep spark || echo "❌ Aucun container Spark actif"

spark-test: ## Teste la connexion Spark depuis Airflow
	@echo "🧪 Test de connexion Spark..."
	@$(DE) $(SERVICE) python /opt/airflow/tools/test_spark_connection.py

spark-restart: ## Redémarre le cluster Spark
	@echo "🔄 Redémarrage du cluster Spark..."
	@$(DC) restart spark-master spark-worker-1 spark-worker-2
	@echo "✅ Cluster Spark redémarré"

rebuild-spark: ## Rebuild l'image Airflow avec PySpark
	@echo "🔨 Rebuild de l'image Airflow avec PySpark..."
	@$(DC) build airflow-apiserver
	@echo "✅ Image rebuildée. Exécutez 'make restart' pour appliquer les changements."



