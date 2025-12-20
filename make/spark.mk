# ====================================
# Commandes Spark
# ====================================

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
