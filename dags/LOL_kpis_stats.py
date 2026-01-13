import sys
import os
import logging

from typing import Any
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, Asset, TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.databases as databases
import app.manager as manager
import app.helper as helper

from app.tasks.decorateurs import customTask
from airflow.exceptions import AirflowFailException, AirflowSkipException

DAG_ID = "LOL_kpis_stats"
DESCRIPTION = ""
OBJECTIF = ""
SCHEDULE = None
START_DATE = datetime(2025, 1, 1)
TAGS = []

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=10), # Temps entre chaque tentative
}

class Custom():

    @customTask
    @staticmethod
    def kpi(
        xcom_source: str,
        per_player: bool = False,
        per_champion: bool = False,
        **kwargs
    ) -> Any:
        """ Calcul des KPI de performance des joueurs par champion.

        Args:
            xcom_source (str): Source XCom pour récupérer les données de statistiques.

        Returns:
            Any: Résultat du calcul des KPI stocké dans XCom.
                - avg_damage_per_minute: Dégâts moyens par minute.
                - avg_physical_damage_per_minute: Dégâts physiques moyens par minute.
                - avg_magic_damage_per_minute: Dégâts magiques moyens par minute.
                - avg_true_damage_per_minute: Dégâts vrais moyens par minute.
                - avg_physical_damage_dealt_pct: Pourcentage moyen de dégâts physiques infligés
                - avg_magic_damage_dealt_pct: Pourcentage moyen de dégâts magiques infligés
                - avg_true_damage_dealt_pct: Pourcentage moyen de dégâts vrais infligés
                - avg_kills_per_minute: Kills moyens par minute.
                - avg_deaths_per_minute: Deaths moyens par minute.
                - avg_assists_per_minute: Assists moyens par minute.
                - avg_multi_kill_score: Score moyen de multi-kills.
                - penta_rate: Taux de penta-kills.
                - avg_damage_taken_per_death: Dégâts moyens subis par mort.
                - avg_physical_damage_taken_pct: Pourcentage moyen de dégâts physiques subis
                - avg_magic_damage_taken_pct: Pourcentage moyen de dégâts magiques subis
                - avg_true_damage_taken_pct: Pourcentage moyen de dégâts vrais subis
                - avg_heal_per_minute: Soins moyens par minute.
                - avg_cs_per_minute: Farm moyen (minions tués) par minute.
                - avg_gold_per_minute: Or moyen gagné par minute.
                - avg_gold_per_cs: Or moyen gagné par minion tué.
                - avg_kda: Ratio KDA moyen.
                - total_games: Nombre total de parties analysées.
                - best_kills: Meilleur nombre de kills dans une partie.
                - best_damage: Meilleur total de dégâts infligés dans une partie.
                - best_deaths: Meilleur (minimum) nombre de deaths dans une partie.
                - tanking_index: Indice de performance en tanking.
                - damage_index: Indice de performance en dégâts.
                - support_index: Indice de performance en support.

                - game_name: Nom du joueur. Si per_player = True
                - champion_name: Nom du champion. Si per_champion = True
                - champion_id: ID du champion. Si per_champion = True

        Exemple:
            >>> kpi = Custom.kpi(
            ...     task_id="kpi_task",
            ...     xcom_source="get_stats_task",
            ... )
            Les KPI calculés seront accessibles via XCom.
        
        """
        df_stats = manager.Xcom.get(
            xcom_source=xcom_source,
            **kwargs
        )
        if df_stats is None:
            raise AirflowSkipException("Aucune donnée extraite pour le calcul des KPI.")
        
        if not per_player and not per_champion:
            raise AirflowFailException("Place holer erreur !")

        spark = manager.Spark.get(
            app_name="LOL_kpis_stats",
            driver_memory="2g",
            sql_shuffle_partitions="4",
            jars_packages="org.postgresql:postgresql:42.7.1",
            **kwargs
        )

        df_spark = spark.createDataFrame(df_stats)
        df_spark.createOrReplaceTempView("stats")

        logging.info("🔥 Début du calcul des KPI de dégâts par minute par joueur/champion")
        query = """
            SELECT 
                AVG(total_damage_dealt_to_champions / (game_duration / 60)) AS avg_damage_per_minute,
                AVG(physical_damage_dealt_to_champions / (game_duration / 60)) AS avg_physical_damage_per_minute,
                AVG(magic_damage_dealt_to_champions / (game_duration / 60)) AS avg_magic_damage_per_minute,
                AVG(true_damage_dealt_to_champions / (game_duration / 60)) AS avg_true_damage_per_minute,
                
                -- Répartition des dégâts infligés (%)
                AVG(physical_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_physical_damage_dealt_pct,
                AVG(magic_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_magic_damage_dealt_pct,
                AVG(true_damage_dealt_to_champions * 100.0 / GREATEST(total_damage_dealt_to_champions, 1)) AS avg_true_damage_dealt_pct,
                
                -- KDA par minute
                AVG(kills / (game_duration / 60)) AS avg_kills_per_minute,
                AVG(deaths / (game_duration / 60)) AS avg_deaths_per_minute,
                AVG(assists / (game_duration / 60)) AS avg_assists_per_minute,
                
                -- Multi-kill score (score pondéré des multi-kills)
                AVG(
                    (double_kills * 2) + 
                    (triple_kills * 4) + 
                    (quadra_kills * 8) + 
                    (penta_kills * 16)
                ) AS avg_multi_kill_score,
                
                -- Penta rate (pourcentage de parties avec penta)
                (SUM(CASE WHEN penta_kills > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS penta_rate,
                
                -- Dégâts subis par mort
                AVG(total_damage_taken / GREATEST(deaths, 1)) AS avg_damage_taken_per_death,

                AVG(physical_damage_taken / (game_duration / 60)) AS avg_physical_damage_taken_per_minute,
                AVG(magic_damage_taken / (game_duration / 60)) AS avg_magic_damage_taken_per_minute,
                AVG(true_damage_taken / (game_duration / 60)) AS avg_true_damage_taken_per_minute,
                
                -- Répartition des dégâts subis (%)
                AVG(physical_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_physical_damage_taken_pct,
                AVG(magic_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_magic_damage_taken_pct,
                AVG(true_damage_taken * 100.0 / GREATEST(total_damage_taken, 1)) AS avg_true_damage_taken_pct,
                
                -- Sustain (soins + mitigation par minute)
                AVG(total_heal / (game_duration / 60)) AS avg_heal_per_minute,
                
                -- Farm et économie
                AVG(total_minions_killed / (game_duration / 60)) AS avg_cs_per_minute,
                AVG(gold_earned / (game_duration / 60)) AS avg_gold_per_minute,
                AVG(gold_earned / GREATEST(total_minions_killed, 1)) AS avg_gold_per_cs,
                
                -- Statistiques additionnelles
                AVG((kills + assists) / GREATEST(deaths, 1)) AS avg_kda,
                COUNT(*) AS total_games,
                
                -- Statistiques par champion
                MAX(kills) AS best_kills,
                MAX(total_damage_dealt_to_champions) AS best_damage,
                MIN(deaths) AS best_deaths,
                
                -- Indices de performance
                AVG(
                    (total_damage_taken / (game_duration / 60)) * 0.35
                    + (total_heal / (game_duration / 60)) * 0.25
                    + (1.0 / GREATEST(deaths / (game_duration / 60), 0.1)) * 0.3
                    - (deaths / (game_duration / 60)) * 50
                ) AS tanking_index,
                
                AVG(
                    (total_damage_dealt_to_champions / (game_duration / 60)) * 0.6
                    + (kills / (game_duration / 60)) * 80
                    + (double_kills * 2 + triple_kills * 4 + quadra_kills * 8 + penta_kills * 16) * 4
                    - (deaths / (game_duration / 60)) * 50
                ) AS damage_index,
                
                AVG(
                    (assists / (game_duration / 60)) * 180
                    + (total_heal / (game_duration / 60)) * 0.15
                    - (deaths / (game_duration / 60)) * 80
                ) AS support_index,
                
                -- Informations de groupement
                {{params}}
            FROM 
                stats
            WHERE 
                game_duration > 120 -- Filtrer les parties de plus de 2 minutes
            GROUP BY 
                {{params}}
            ORDER BY 
                {{params}}, total_games DESC
        """

        params_fields = []

        if per_player:
            params_fields.extend(["game_name"])
        if per_champion:
            params_fields.extend(["champion_name", "champion_id"])

        kpi_dpm = spark.sql(
            helper.render_jinja2(
                query,
                params={
                    "params": ", ".join(params_fields),
                }
            )
        )

        logging.info("✅ Calcul des KPI de dégâts par minute par joueur/champion terminé")
        kpi_dpm.show()

        logging.info("🔥 Conversion des résultats en Pandas DataFrame ...")
        df_pandas = kpi_dpm.toPandas()

        return manager.Xcom.put(
            input=df_pandas,
            **kwargs
        )

# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=START_DATE, # Date de début du DAG
    #schedule=SCHEDULE,  # Fréquence d'exécution (CRON ou timedelta)
    tags=TAGS, # Liste de tags pour catégoriser le DAG dans l'UI
    catchup=False, # Exécution des tâches manquées (True ou False)
    max_active_runs=1,  # Limite à 1 exécutions actives en même temps
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md=f"""
        ## 🔹 Description
        {DESCRIPTION}

        ## 🔹 Objectif
        {OBJECTIF}
    """,
) as dag:
    
    match_columns = {
        "avg_damage_per_minute": "avg_damage_per_minute",
        "avg_physical_damage_per_minute": "avg_physical_damage_per_minute",
        "avg_magic_damage_per_minute": "avg_magic_damage_per_minute",
        "avg_true_damage_per_minute": "avg_true_damage_per_minute",
        "avg_physical_damage_dealt_pct": "avg_physical_damage_dealt_pct",
        "avg_magic_damage_dealt_pct": "avg_magic_damage_dealt_pct",
        "avg_true_damage_dealt_pct": "avg_true_damage_dealt_pct",
        "avg_kills_per_minute": "avg_kills_per_minute",
        "avg_deaths_per_minute": "avg_deaths_per_minute",
        "avg_assists_per_minute": "avg_assists_per_minute",
        "avg_multi_kill_score": "avg_multi_kill_score",
        "penta_rate": "penta_rate",
        "avg_damage_taken_per_death": "avg_damage_taken_per_death",
        "avg_physical_damage_taken_per_minute": "avg_physical_damage_taken_per_minute",
        "avg_magic_damage_taken_per_minute": "avg_magic_damage_taken_per_minute",
        "avg_true_damage_taken_per_minute": "avg_true_damage_taken_per_minute",
        "avg_physical_damage_taken_pct": "avg_physical_damage_taken_pct",
        "avg_magic_damage_taken_pct": "avg_magic_damage_taken_pct",
        "avg_true_damage_taken_pct": "avg_true_damage_taken_pct",
        "avg_heal_per_minute": "avg_heal_per_minute",
        "avg_cs_per_minute": "avg_cs_per_minute",
        "avg_gold_per_minute": "avg_gold_per_minute",
        "avg_gold_per_cs": "avg_gold_per_cs",
        "avg_kda": "avg_kda",
        "total_games": "total_games",
        "best_kills": "best_kills",
        "best_damage": "best_damage",
        "best_deaths": "best_deaths",
        "tanking_index": "tanking_index",
        "damage_index": "damage_index",
        "support_index": "support_index",
    }

    get_full_stats_by_puuid_to_process = databases.PostgresWarehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        schema="lol_fact_datas",
        table_name="lol_fact_stats",
        task_id="get_full_stats_by_puuid_to_process",
        schema_select=[
            "t_main.*",
            "puuid.game_name",
            "match.game_duration",
        ],
        joins=[
            {
                'table': 'lol_fact_datas.lol_fact_puuid_to_process',
                'alias': 'puuid',
                'on': 't_main.puuid = puuid.puuid',
                'type': 'INNER'
            },
            {
                'table': 'lol_fact_datas.lol_fact_match',
                'alias': 'match',
                'on': 't_main.match_id = match.match_id',
                'type': 'INNER'
            },
        ],
    )

    get_full_stats_by_champion = databases.PostgresWarehouse.extract(
        engine=manager.Connectors.postgres("POSTGRES_warehouse"),
        schema="lol_fact_datas",
        table_name="lol_fact_stats",
        task_id="get_full_stats_by_champion",
        schema_select=[
            "t_main.*",
            "match.game_duration",
        ],
        joins=[
            {
                'table': 'lol_fact_datas.lol_fact_match',
                'alias': 'match',
                'on': 't_main.match_id = match.match_id',
                'type': 'INNER'
            },
        ],
    )

    with TaskGroup("groupe_per_player") as groupe_per_player:

        task_kpi_per_player = Custom.kpi(
            task_id = "task_kpi_per_player",
            xcom_source = "get_full_stats_by_puuid_to_process",
            per_champion = False,
            per_player = True,
        )

        insert_lol_raw_kpi_per_player = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_kpi_per_player",
            xcom_source="groupe_per_player.task_kpi_per_player",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_kpi_per_player",
            schema="lol_raw_datas",
            if_table_exists="replace",
        )

        non_match_columns_per_player = match_columns.copy()
        non_match_columns_per_player["game_name"] = "game_name"

        # Transformation des données brutes en données factuelles
        raw_to_fact_kpi_per_player = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact_kpi_per_player",
            outlets=[Asset('warehouse://lol_mart_datas/lol_kpi_stats_per_player')],
            source_table="lol_raw_datas.lol_raw_kpi_per_player",
            target_table="lol_mart_datas.lol_kpi_stats_per_player",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=True,
            join_keys=["game_name"],
            match_columns=match_columns,
            non_match_columns=non_match_columns_per_player,
        )
        
        chain(
            task_kpi_per_player,
            insert_lol_raw_kpi_per_player,
            raw_to_fact_kpi_per_player,
        )

    with TaskGroup("groupe_per_player_champion") as groupe_per_player_champion:

        task_kpi_per_player_champion = Custom.kpi(
            task_id = "task_kpi_per_player_champion",
            xcom_source = "get_full_stats_by_puuid_to_process",
            per_champion = True,
            per_player = True,
        )

        # Insertion des données brutes dans la table d'entrepôt
        insert_lol_raw_kpi_per_player_champion = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_kpi_per_player_champion",
            xcom_source="groupe_per_player_champion.task_kpi_per_player_champion",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_kpi_per_player_champion",
            schema="lol_raw_datas",
            if_table_exists="replace",
        )

        non_match_columns_per_player_champion = match_columns.copy()
        non_match_columns_per_player_champion["game_name"] = "game_name"
        non_match_columns_per_player_champion["champion_name"] = "champion_name"
        non_match_columns_per_player_champion["champion_id"] = "champion_id"

        # Transformation des données brutes en données factuelles
        raw_to_fact_kpi_per_player_champion = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact_kpi_per_player_champion",
            outlets=[Asset('warehouse://lol_mart_datas/lol_kpi_stats_per_player_champion')],
            source_table="lol_raw_datas.lol_raw_kpi_per_player_champion",
            target_table="lol_mart_datas.lol_kpi_stats_per_player_champion",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=True,
            join_keys=["game_name", "champion_id"],
            match_columns=match_columns,
            non_match_columns=non_match_columns_per_player_champion,
        )
        chain(
            task_kpi_per_player_champion,
            insert_lol_raw_kpi_per_player_champion,
            raw_to_fact_kpi_per_player_champion,
        )

    with TaskGroup("groupe_per_champion") as groupe_per_champion:

        task_kpi_per_champion = Custom.kpi(
            task_id = "task_kpi_per_champion",
            xcom_source = "get_full_stats_by_champion",
            per_champion = True,
            per_player = False,
        )

        # Insertion des données brutes dans la table d'entrepôt
        insert_lol_raw_kpi_per_champion = databases.PostgresWarehouse.insert(
            task_id="insert_lol_raw_kpi_per_champion",
            xcom_source="groupe_per_champion.task_kpi_per_champion",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            table_name="lol_raw_kpi_per_champion",
            schema="lol_raw_datas",
            if_table_exists="replace",
        )

        non_match_columns_per_champion = match_columns.copy()
        non_match_columns_per_champion["champion_name"] = "champion_name"
        non_match_columns_per_champion["champion_id"] = "champion_id"

        # Transformation des données brutes en données factuelles
        raw_to_fact_kpi_per_champion = databases.PostgresWarehouse.raw_to_fact(
            task_id="raw_to_fact_kpi_per_champion",
            outlets=[Asset('warehouse://lol_mart_datas/lol_kpi_stats_per_champion')],
            source_table="lol_raw_datas.lol_raw_kpi_per_champion",
            target_table="lol_mart_datas.lol_kpi_stats_per_champion",
            engine=manager.Connectors.postgres("POSTGRES_warehouse"),
            has_not_matched=True,
            has_matched=True,
            join_keys=["champion_id"],
            match_columns=match_columns,
            non_match_columns=non_match_columns_per_champion,
        )
        chain(
            task_kpi_per_champion,
            insert_lol_raw_kpi_per_champion,
            raw_to_fact_kpi_per_champion,
        )

    chain(
        get_full_stats_by_puuid_to_process,
        [groupe_per_player, groupe_per_player_champion],
    )
    chain(
        get_full_stats_by_champion,
        groupe_per_champion,
    )
