# ====================================
# Commandes pour la gestion de Marquez
# ====================================

clean_db: ## Nettoyer complètement la base de données Marquez
	@echo "🧹 Nettoyage de la base de données Marquez..."
	@echo "⚠️  Ceci va supprimer TOUTES les données de lignage!"
	@bash -c 'read -p "Êtes-vous sûr? [y/N] " -n 1 -r; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo ""; \
		echo "🛑 Arrêt des services Marquez..."; \
		sudo docker compose stop marquez marquez-web; \
		echo "🗑️  Suppression du volume de base de données..."; \
		sudo docker volume rm hutchai-airflow_marquez-db-volume 2>/dev/null || true; \
		echo "🚀 Redémarrage des services Marquez..."; \
		sudo docker compose up -d marquez-db; \
		echo "⏳ Attente de la base de données..."; \
		sleep 10; \
		sudo docker compose up -d marquez marquez-web; \
		echo "✅ Base de données Marquez nettoyée et redémarrée!"; \
		echo "🌐 Interface disponible sur http://localhost:3001"; \
	else \
		echo ""; \
		echo "❌ Opération annulée."; \
	fi'

clean_tables: ## Vider les tables Marquez sans recréer le volume
	@echo "🧹 Nettoyage des tables Marquez..."
	@sudo docker compose exec marquez-db psql -U marquez -d marquez -c "\
		TRUNCATE TABLE lineage_events CASCADE; \
		TRUNCATE TABLE runs CASCADE; \
		TRUNCATE TABLE jobs CASCADE; \
		TRUNCATE TABLE datasets CASCADE; \
		TRUNCATE TABLE dataset_versions CASCADE; \
		TRUNCATE TABLE namespaces CASCADE;" 2>/dev/null || echo "⚠️  Certaines tables n'existent pas encore"
	@echo "✅ Tables nettoyées!"
	
restart_marquez: ## Redémarrer les services Marquez
	@echo "🔄 Redémarrage des services Marquez..."
	@sudo docker compose restart marquez marquez-web
	@echo "✅ Services Marquez redémarrés!"