import logging
import socket

from pyspark.sql import SparkSession
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowFailException

import app.helper as helper

class Spark:
    """Gestionnaire de SparkSession pour Airflow."""

    __session: dict[str, SparkSession] = {}

    @staticmethod
    def get(
        conn_id: str = "spark_default",
        app_name: str = "default_task_id",
        driver_memory: str = "1g",
        driver_max_result_size: str = "1g",
        driver_bind_address: str = "0.0.0.0",
        sql_adaptive_enabled: str = "true",
        sql_adaptive_coalesce_partitions_enabled: str = "true",
        serializer: str = "org.apache.spark.serializer.KryoSerializer",
        sql_shuffle_partitions: str = "8",
        executor_heartbeat_interval: str = "60s",
        network_timeout: str = "300s",
        spark_configs: dict = None,
        **kwargs
    ) -> SparkSession:
        """Retourne la SparkSession existante ou en crée une nouvelle si nécessaire.

        Args:
            conn_id: ID de la connexion Airflow Spark
            app_name: Nom de l'application Spark (par défaut: task_id)
            driver_memory: Mémoire allouée au driver
            driver_max_result_size: Taille maximale des résultats du driver
            driver_bind_address: Adresse de bind du driver
            sql_adaptive_enabled: Active l'exécution adaptative SQL
            sql_adaptive_coalesce_partitions_enabled: Active la coalescence des partitions
            serializer: Sérialiseur Spark à utiliser
            sql_shuffle_partitions: Nombre de partitions pour les shuffles
            executor_heartbeat_interval: Intervalle de heartbeat des executors
            network_timeout: Timeout réseau
            spark_configs: Dictionnaire de configurations Spark supplémentaires
            **kwargs: Arguments additionnels (task_id, etc.)

        Returns:
            SparkSession configurée
        """

        helper.logging_title("⏳ Configuration de la SparkSession", lvl=3)

        task_id = kwargs.get("task_id", "default_task_id")
        app_name = app_name or task_id

        if app_name not in Spark.__session and not Spark.__set(
            conn_id=conn_id,
            app_name=app_name,
            driver_memory=driver_memory,
            driver_max_result_size=driver_max_result_size,
            driver_bind_address=driver_bind_address,
            sql_adaptive_enabled=sql_adaptive_enabled,
            sql_adaptive_coalesce_partitions_enabled=sql_adaptive_coalesce_partitions_enabled,
            serializer=serializer,
            sql_shuffle_partitions=sql_shuffle_partitions,
            executor_heartbeat_interval=executor_heartbeat_interval,
            network_timeout=network_timeout,
            spark_configs=spark_configs,
        ):
            raise AirflowFailException("Impossible de créer une SparkSession Spark.")

        helper.logging_title("✅ SparkSession obtenue.", lvl=3, close=True)

        return Spark.__session[app_name]

    @staticmethod
    def close(
        app_name: str = "default_task_id",
        **kwargs
    ) -> None:
        """Ferme la SparkSession proprement.

        Args:
            app_name: Nom de l'application Spark (par défaut: task_id)
            **kwargs: Arguments additionnels (task_id, etc.)
        """

        task_id = kwargs.get("task_id", "default_task_id")
        app_name = app_name or task_id

        if app_name not in Spark.__session:
            logging.warning("⚠️ Aucune SparkSession active à fermer.")
            return

        try:
            logging.info("⏳ Fermeture de la SparkSession...")
            Spark.__session[app_name].stop()
            logging.info("✅ SparkSession fermée.")

            del Spark.__session[app_name]
            logging.info("✅ SparkSession réinitialisée.")

        except Exception as e:
            raise AirflowFailException("❌ Échec de la fermeture de la SparkSession.") from e

        finally:
            if app_name in Spark.__session:
                del Spark.__session[app_name]  # Réinitialiser quand même
            return

    @staticmethod
    def __set(
        conn_id: str = "spark_default",
        app_name: str = "default_task_id",
        driver_memory: str = "1g",
        driver_max_result_size: str = "1g",
        driver_bind_address: str = "0.0.0.0",
        sql_adaptive_enabled: str = "true",
        sql_adaptive_coalesce_partitions_enabled: str = "true",
        serializer: str = "org.apache.spark.serializer.KryoSerializer",
        sql_shuffle_partitions: str = "8",
        executor_heartbeat_interval: str = "60s",
        network_timeout: str = "300s",
        spark_configs: dict = None
    ) -> bool:
        """Configure et retourne une SparkSession à partir d'une connexion Airflow."""

        if app_name in Spark.__session:
            logging.warning("⚠️ Une SparkSession existe déjà. Utilisation de la session existante.")
            return False

        try:
            # Récupérer la connexion Spark depuis Airflow
            conn = BaseHook.get_connection(conn_id)
            host = conn.host
            port = conn.port

            # Récupérer les configurations depuis extra (JSON)
            extra = conn.extra_dejson
            executor_memory = extra.get("executor-memory", "1g")
            total_executor_cores = extra.get("total-executor-cores", "2")
            executor_cores = extra.get("executor-cores", "2")

            # Obtenir l'adresse IP du conteneur
            hostname = socket.gethostname()
            host_ip = socket.gethostbyname(hostname)

            logging.info(f"ℹ️ Driver hostname: {hostname}, IP: {host_ip}")
            logging.info(f"ℹ️ Connecting to Spark cluster at spark://{host}:{port}")

            # Créer la session Spark avec configuration optimisée
            builder = SparkSession.builder \
                .appName(app_name) \
                .master(f"spark://{host}:{port}") \
                .config("spark.driver.host", host_ip) \
                .config("spark.driver.bindAddress", driver_bind_address) \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.executor.cores", executor_cores) \
                .config("spark.cores.max", total_executor_cores) \
                .config("spark.driver.memory", driver_memory) \
                .config("spark.driver.maxResultSize", driver_max_result_size) \
                .config("spark.sql.adaptive.enabled", sql_adaptive_enabled) \
                .config("spark.sql.adaptive.coalescePartitions.enabled", sql_adaptive_coalesce_partitions_enabled) \
                .config("spark.serializer", serializer) \
                .config("spark.default.parallelism", str(int(total_executor_cores) * 2)) \
                .config("spark.sql.shuffle.partitions", sql_shuffle_partitions) \
                .config("spark.executor.heartbeatInterval", executor_heartbeat_interval) \
                .config("spark.network.timeout", network_timeout)

            # Ajouter les configurations supplémentaires si fournies
            if spark_configs:
                for key, value in spark_configs.items():
                    builder = builder.config(key, value)

            Spark.__session[app_name] = builder.getOrCreate()

            # Vérifier que les executors sont bien connectés
            sc = Spark.__session[app_name].sparkContext
            logging.info("🔵 VÉRIFICATION DE LA CONNEXION SPARK 🔵")
            logging.info(f"🔹Application ID: {sc.applicationId}")
            logging.info(f"🔹Master URL: {sc.master}")
            logging.info(f"🔹Default parallelism: {sc.defaultParallelism}")

            # Log de la configuration finale
            logging.info("🔵 CONFIGURATION SPARK ACTIVE 🔵")
            logging.info(f"🔹Executors configurés: {total_executor_cores} cores total")
            logging.info(f"🔹Mémoire par executor: {executor_memory}")
            logging.info(f"🔹Parallelism: {sc.defaultParallelism}")

            return True

        except Exception as e:
            raise AirflowFailException(f"❌ Échec de connexion au cluster Spark {conn_id} ! {e}")
