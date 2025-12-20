# ====================================
# Commandes de vérification
# ====================================

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
