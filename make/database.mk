# ====================================
# Commandes Database
# ====================================

setup-db: setup-schema setup_table_lol_fact_match setup_table_lol_fact_puuid setup_table_lol_kpi_stats_per_player_champion setup_table_lol_kpi_stats_per_player setup_table_lol_fact_puuid_to_process setup-table-lol_fact_stats setup-data-puuid ## Initialise la base de données du warehouse avec les schémas et tables nécessaires
	@echo "✅ Initialisation complète de la base de données du warehouse réussie !"

setup-schema:
	@echo "🔨 Création du schéma lol_fact_datas dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE SCHEMA IF NOT EXISTS lol_fact_datas;" || { echo "❌ Échec de la création du schéma lol_fact_datas"; exit 1; }
	@echo "✅ Schéma lol_fact_datas créé avec succès !"

	@echo "🔨 Création du schéma lol_mart_datas dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE SCHEMA IF NOT EXISTS lol_mart_datas;" || { echo "❌ Échec de la création du schéma lol_mart_datas"; exit 1; }
	@echo "✅ Schéma lol_mart_datas créé avec succès !"

	@echo "🔨 Création du schéma lol_raw_datas dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE SCHEMA IF NOT EXISTS lol_raw_datas;" || { echo "❌ Échec de la création du schéma lol_raw_datas"; exit 1; }
	@echo "✅ Schéma lol_raw_datas créé avec succès !"

setup_table_lol_fact_match: ## Crée la table lol_fact_match dans le warehouse
	@echo "🔨 Création de la table lol_fact_match dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_fact_datas.lol_fact_match ( \
			match_id VARCHAR(50) PRIMARY KEY, \
			puuid_1 VARCHAR(250) DEFAULT NULL, \
			puuid_2 VARCHAR(250) DEFAULT NULL, \
			puuid_3 VARCHAR(250) DEFAULT NULL, \
			puuid_4 VARCHAR(250) DEFAULT NULL, \
			puuid_5 VARCHAR(250) DEFAULT NULL, \
			puuid_6 VARCHAR(250) DEFAULT NULL, \
			puuid_7 VARCHAR(250) DEFAULT NULL, \
			puuid_8 VARCHAR(250) DEFAULT NULL, \
			puuid_9 VARCHAR(250) DEFAULT NULL, \
			puuid_10 VARCHAR(250) DEFAULT NULL, \
			game_creation TIMESTAMP DEFAULT NULL, \
			game_duration BIGINT DEFAULT NULL, \
			game_mode VARCHAR(50) DEFAULT NULL, \
			game_version VARCHAR(20) DEFAULT NULL, \
			game_in_progress BOOLEAN DEFAULT FALSE, \
			is_processed BOOLEAN DEFAULT FALSE, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_fact_match"; exit 1; }
	@echo "✅ Table lol_fact_match créée avec succès !"

setup_table_lol_fact_puuid_to_process: ## Crée la table lol_fact_puuid_to_process dans le warehouse
	@echo "🔨 Création de la table lol_fact_puuid_to_process dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_fact_datas.lol_fact_puuid_to_process ( \
			puuid VARCHAR(250) PRIMARY KEY, \
			game_name VARCHAR(100) DEFAULT NULL, \
			tag_line VARCHAR(50) DEFAULT NULL, \
			date_processed TIMESTAMP DEFAULT NULL, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_fact_puuid_to_process"; exit 1; }
	@echo "✅ Table lol_fact_puuid_to_process créée avec succès !"

setup_table_lol_fact_puuid: ## Crée la table lol_fact_puuid dans le warehouse
	@echo "🔨 Création de la table lol_fact_puuid dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_fact_datas.lol_fact_puuid ( \
			puuid VARCHAR(250) PRIMARY KEY, \
			game_name VARCHAR(100) DEFAULT NULL, \
			tag_line VARCHAR(50) DEFAULT NULL, \
			queue_type VARCHAR(30) DEFAULT NULL, \
			tier VARCHAR(10) DEFAULT NULL, \
			rank VARCHAR(4) DEFAULT NULL, \
			date_processed TIMESTAMP DEFAULT NULL, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_fact_puuid"; exit 1; }
	@echo "✅ Table lol_fact_puuid créée avec succès !"

setup_table_lol_kpi_stats_per_player_champion: ## Crée la table lol_kpi_stats_per_player_champion dans le warehouse
	@echo "🔨 Création de la table lol_kpi_stats_per_player_champion dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_mart_datas.lol_kpi_stats_per_player_champion ( \
			avg_damage_per_minute double precision, \
			avg_physical_damage_per_minute double precision, \
			avg_magic_damage_per_minute double precision, \
			avg_true_damage_per_minute double precision, \
			avg_physical_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_magic_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_true_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_kills_per_minute double precision, \
			avg_deaths_per_minute double precision, \
			avg_assists_per_minute double precision, \
			avg_multi_kill_score double precision, \
			penta_rate text COLLATE pg_catalog."default", \
			avg_damage_taken_per_death double precision, \
			avg_physical_damage_taken_per_minute double precision, \
			avg_magic_damage_taken_per_minute double precision, \
			avg_true_damage_taken_per_minute double precision, \
			avg_physical_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_magic_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_true_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_heal_per_minute double precision, \
			avg_cs_per_minute double precision, \
			avg_gold_per_minute double precision, \
			avg_gold_per_cs double precision, \
			avg_kda double precision, \
			total_games bigint, \
			best_kills bigint, \
			best_damage bigint, \
			best_deaths bigint, \
			tanking_index double precision, \
			damage_index double precision, \
			support_index double precision, \
			game_name text COLLATE pg_catalog."default", \
			champion_name text COLLATE pg_catalog."default", \
			champion_id bigint, \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_kpi_stats"; exit 1; }
	@echo "✅ Table lol_kpi_stats créée avec succès !"

setup_table_lol_kpi_stats_per_player: ## Crée la table lol_kpi_stats_per_player dans le warehouse
	@echo "🔨 Création de la table lol_kpi_stats_per_player dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_mart_datas.lol_kpi_stats_per_player ( \
			avg_damage_per_minute double precision, \
			avg_physical_damage_per_minute double precision, \
			avg_magic_damage_per_minute double precision, \
			avg_true_damage_per_minute double precision, \
			avg_physical_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_magic_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_true_damage_dealt_pct text COLLATE pg_catalog."default", \
			avg_kills_per_minute double precision, \
			avg_deaths_per_minute double precision, \
			avg_assists_per_minute double precision, \
			avg_multi_kill_score double precision, \
			penta_rate text COLLATE pg_catalog."default", \
			avg_damage_taken_per_death double precision, \
			avg_physical_damage_taken_per_minute double precision, \
			avg_magic_damage_taken_per_minute double precision, \
			avg_true_damage_taken_per_minute double precision, \
			avg_physical_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_magic_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_true_damage_taken_pct text COLLATE pg_catalog."default", \
			avg_heal_per_minute double precision, \
			avg_cs_per_minute double precision, \
			avg_gold_per_minute double precision, \
			avg_gold_per_cs double precision, \
			avg_kda double precision, \
			total_games bigint, \
			best_kills bigint, \
			best_damage bigint, \
			best_deaths bigint, \
			tanking_index double precision, \
			damage_index double precision, \
			support_index double precision, \
			game_name text COLLATE pg_catalog."default", \
			tech_date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
		);" || { echo "❌ Échec de la création de la table lol_kpi_stats"; exit 1; }
	@echo "✅ Table lol_kpi_stats créée avec succès !"


setup-table-lol_fact_stats: ## Crée la table lol_fact_stats dans le warehouse
	@echo "🔨 Création de la table lol_fact_stats dans le warehouse..."
	@$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		CREATE TABLE IF NOT EXISTS lol_fact_datas.lol_fact_stats ( \
			id VARCHAR(250) PRIMARY KEY, \
			match_id VARCHAR(50), \
			puuid VARCHAR(250), \
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
			tech_date_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
			item0 INTEGER, \
			item1 INTEGER, \
			item2 INTEGER, \
			item3 INTEGER, \
			item4 INTEGER, \
			item5 INTEGER, \
			item6 INTEGER \
		);" || { echo "❌ Échec de la création du schéma ou des tables"; exit 1; }

setup-data-puuid: ## Insère des données initiales dans la table lol_fact_puuid_to_process
	@echo "🔨 Récupération du puuid depuis l'API Riot Games..."
	@RESPONSE=$$(curl -s "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/JeanPomme/POMM?api_key=${RIOT_API_KEY}"); \
	PUUID=$$(echo "$$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('puuid', ''))" 2>/dev/null); \
	GAME_NAME=$$(echo "$$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('gameName', ''))" 2>/dev/null); \
	TAG_LINE=$$(echo "$$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('tagLine', ''))" 2>/dev/null); \
	if [ -z "$$PUUID" ] || [ "$$PUUID" = "null" ]; then \
		echo "❌ Échec de la récupération du puuid depuis l'API"; \
		echo "📋 Réponse API: $$RESPONSE"; \
		exit 1; \
	fi; \
	echo "✅ PUUID récupéré: $$PUUID"; \
	echo "🔨 Insertion du puuid dans la table lol_fact_puuid_to_process..."; \
	$(DE) $(WAREHOUSE) psql -U warehouse -d warehouse -c "\
		INSERT INTO lol_fact_datas.lol_fact_puuid_to_process (puuid, game_name, tag_line) \
		VALUES \
		('$$PUUID', '$$GAME_NAME', '$$TAG_LINE') \
		ON CONFLICT (puuid) DO NOTHING;" || { echo "❌ Échec de l'insertion des données initiales dans la table lol_fact_puuid_to_process"; exit 1; }
	@echo "✅ Données initiales insérées avec succès dans la table lol_fact_puuid_to_process !"