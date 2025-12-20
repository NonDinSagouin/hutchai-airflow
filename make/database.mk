# ====================================
# Commandes Database
# ====================================

setup-db: setup-schema setup_table_lol_fact_match_datas setup_table_lol_fact_puuid setup-table-lol_fact_stats ## Initialise la base de données du warehouse avec les schémas et tables nécessaires
	@echo "✅ Initialisation complète de la base de données du warehouse réussie !"
	
setup-schema:
	@echo "🔨 Création du schéma lol_datas dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE SCHEMA IF NOT EXISTS lol_datas;" || { echo "❌ Échec de la création du schéma lol_datas"; exit 1; }
	@echo "✅ Schéma lol_datas créé avec succès !"

setup_table_lol_fact_match_datas: ## Crée la table lol_fact_match_datas dans le warehouse
	@echo "🔨 Création de la table lol_fact_match_datas dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_datas.lol_fact_match_datas ( \
			match_id VARCHAR(50) PRIMARY KEY, \
			game_creation BIGINT DEFAULT NULL, \
			game_duration BIGINT DEFAULT NULL, \
			game_mode VARCHAR(50) DEFAULT NULL, \
			game_version VARCHAR(20) DEFAULT NULL, \
			game_in_progress BOOLEAN DEFAULT FALSE, \
			is_processed BOOLEAN DEFAULT FALSE, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_fact_match_datas"; exit 1; }
	@echo "✅ Table lol_fact_match_datas créée avec succès !"

setup_table_lol_fact_puuid: ## Crée la table lol_fact_puuid dans le warehouse
	@echo "🔨 Création de la table lol_fact_puuid dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_datas.lol_fact_puuid ( \
			puuid VARCHAR(250) PRIMARY KEY, \
			queue_type VARCHAR(30) DEFAULT 'unranked', \
			tier VARCHAR(10) DEFAULT NULL, \
			rank VARCHAR(4) DEFAULT NULL, \
			wins INTEGER DEFAULT NULL, \
			losses INTEGER DEFAULT NULL, \
			date_processed TIMESTAMP DEFAULT NULL, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_fact_puuid"; exit 1; }
	@echo "✅ Table lol_fact_puuid créée avec succès !"

setup-table-lol_fact_stats: ## Crée la table lol_fact_stats dans le warehouse
	@echo "🔨 Création de la table lol_fact_puuid dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_datas.lol_fact_stats ( \
			id VARCHAR(250) PRIMARY KEY, \
			match_id VARCHAR(50), \
			puuid VARCHAR(250), \
			game_creation VARCHAR(50), \
			game_version VARCHAR(20), \
			game_mode VARCHAR(25), \
			champion_id INTEGER, \
			champion_name VARCHAR(100), \
			kills INTEGER, \
			deaths INTEGER, \
			assists INTEGER, \
			kda INTEGER, \
			double_kills INTEGER, \
			triple_kills INTEGER, \
			quadra_kills INTEGER, \
			penta_kills INTEGER, \
			largest_killing_spree INTEGER, \
			total_damage_dealt INTEGER, \
			total_damage_dealt_to_champions INTEGER, \
			physical_damage_dealt_to_champions INTEGER, \
			magic_damage_dealt_to_champions INTEGER, \
			true_damage_dealt_to_champions INTEGER, \
			largest_critical_strike INTEGER, \
			total_damage_taken INTEGER, \
			physical_damage_taken INTEGER, \
			magic_damage_taken INTEGER, \
			true_damage_taken INTEGER, \
			total_heal INTEGER, \
			total_heals_on_teammates INTEGER, \
			total_minions_killed INTEGER, \
			neutral_minions_killed INTEGER, \
			gold_earned INTEGER, \
			champ_level INTEGER, \
			champ_experience INTEGER, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création du schéma ou des tables"; exit 1; }