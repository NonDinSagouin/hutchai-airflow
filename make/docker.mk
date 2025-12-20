# ====================================
# Commandes Docker
# ====================================

init: ## Génère le fichier .env puis initialise la base de données Airflow
	$(DC) up airflow-init
	$(DC) -f $(COMPOSE_FILE) up -d --build

	sudo mkdir -p logs
	sudo chmod 777 logs

	sudo mkdir -p temp
	sudo chmod 777 temp
	
	sudo mkdir -p /media/damien/Datas/docker-volumes/warehouse
	sudo chown -R 999:999 /media/damien/Datas/docker-volumes/warehouse

build: ## Construire les images Docker personnalisées
	$(DC) -f $(COMPOSE_FILE) up -d --build

up: ## Lancer les services avec Docker Compose
	$(DC) -f $(COMPOSE_FILE) up -d

down: ## Arrêter et supprimer les services avec Docker Compose
	$(DC) -f $(COMPOSE_FILE) down

restart: ## Arrêter puis lancer les services avec Docker Compose
	make down
	make up

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
