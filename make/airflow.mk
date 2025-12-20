# ====================================
# Commandes Airflow
# ====================================

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
