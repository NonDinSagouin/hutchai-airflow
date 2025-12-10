import logging
import time
import gc

from pyspark.sql import functions as F

from app.tasks.decorateurs import customTask
from app.manager.Spark import Spark

class Stress_test():

    @customTask
    @staticmethod
    def spark_stress_test_memory(**kwargs) -> dict:
        """Test les limites mémoire de Spark."""

        try:
            spark = Spark.get(**kwargs)

            logging.info("=== DÉBUT DU TEST DE STRESS MÉMOIRE SPARK ===")
            logging.info(f"Configuration Spark: {spark.sparkContext.getConf().getAll()}")

            # Test avec des DataFrames de plus en plus volumineux
            results = {}
            total_start_time = time.time()

            # Tailles progressives plus agressives
            sizes = [10_000, 100_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000]

            for i, size in enumerate(sizes, 1):
                logging.info(f"--- TEST {i}/{len(sizes)}: Taille {size:,} lignes ---")
                test_start_time = time.time()

                try:
                    # Créer un DataFrame volumineux avec plus de colonnes
                    logging.info(f"Création du DataFrame de {size:,} lignes...")
                    df = spark.range(size)

                    # Ajouter plusieurs colonnes avec calculs
                    df = df.withColumn("data1", F.rand())
                    df = df.withColumn("data2", F.rand() * 1000)
                    df = df.withColumn("data3", F.rand() * F.col("id"))
                    df = df.withColumn("data4", F.sqrt(F.col("id")))
                    df = df.withColumn("text_col", F.concat(F.lit("row_"), F.col("id").cast("string")))
                    df = df.withColumn("hash_col", F.hash(F.col("text_col")))

                    # Cache pour forcer le chargement en mémoire
                    df = df.cache()
                    logging.info("DataFrame cached, début du count()...")

                    # Forcer l'évaluation et mesurer le temps
                    count_start = time.time()
                    count = df.count()
                    count_time = time.time() - count_start

                    logging.info(f"Count terminé: {count:,} lignes en {count_time:.2f}s")

                    # Test d'agrégations complexes
                    logging.info("Début des agrégations complexes...")
                    agg_start = time.time()

                    agg_result = df.agg(
                        F.sum("data1").alias("sum_data1"),
                        F.avg("data2").alias("avg_data2"),
                        F.max("data3").alias("max_data3"),
                        F.min("data4").alias("min_data4"),
                        F.count("*").alias("total_count")
                    ).collect()[0]

                    agg_time = time.time() - agg_start
                    logging.info(f"Agrégations terminées en {agg_time:.2f}s")

                    # Test de jointure avec lui-même (très coûteux)
                    if size <= 1_000_000:  # Limiter les jointures aux petites tailles
                        logging.info("Test de jointure...")
                        join_start = time.time()

                        df_small = df.limit(1000).withColumnRenamed("id", "join_id")
                        joined_df = df.join(df_small, df.id == df_small.join_id, "inner")
                        join_count = joined_df.count()

                        join_time = time.time() - join_start
                        logging.info(f"Jointure terminée: {join_count:,} lignes en {join_time:.2f}s")
                    else:
                        join_time = 0
                        join_count = 0
                        logging.info("Jointure ignorée (taille trop importante)")

                    # Statistiques mémoire
                    storage_level = df.storageLevel
                    partitions_count = df.rdd.getNumPartitions()

                    test_time = time.time() - test_start_time

                    results[f"size_{size}"] = {
                        "status": "success",
                        "count": count,
                        "partitions": partitions_count,
                        "count_time_seconds": round(count_time, 2),
                        "aggregation_time_seconds": round(agg_time, 2),
                        "join_time_seconds": round(join_time, 2),
                        "join_count": join_count,
                        "total_test_time_seconds": round(test_time, 2),
                        "storage_level": str(storage_level),
                        "aggregation_results": {
                            "sum_data1": float(agg_result["sum_data1"]) if agg_result["sum_data1"] else 0,
                            "avg_data2": float(agg_result["avg_data2"]) if agg_result["avg_data2"] else 0,
                            "max_data3": float(agg_result["max_data3"]) if agg_result["max_data3"] else 0,
                            "min_data4": float(agg_result["min_data4"]) if agg_result["min_data4"] else 0
                        }
                    }

                    logging.info(f"✅ Test {i} réussi: {size:,} lignes en {test_time:.2f}s")
                    logging.info(f"   Partitions: {partitions_count}, Storage: {storage_level}")

                    # Unpersist pour libérer la mémoire
                    df.unpersist()

                    # Garbage collection Python
                    gc.collect()

                except Exception as e:
                    test_time = time.time() - test_start_time
                    error_msg = str(e)

                    logging.error(f"❌ Test {i} échoué pour {size:,} lignes après {test_time:.2f}s")
                    logging.error(f"   Erreur: {error_msg}")

                    results[f"size_{size}"] = {
                        "status": "failed",
                        "error": error_msg,
                        "test_time_seconds": round(test_time, 2),
                        "failed_at_size": size
                    }

                    # Arrêter les tests si on atteint les limites
                    if "OutOfMemoryError" in error_msg or "Cannot allocate" in error_msg:
                        logging.warning("Limite mémoire atteinte, arrêt des tests plus volumineux")
                        break

            total_time = time.time() - total_start_time

            # Résumé final
            successful_tests = [k for k, v in results.items() if v["status"] == "success"]
            failed_tests = [k for k, v in results.items() if v["status"] == "failed"]

            logging.info("=== RÉSUMÉ DU TEST DE STRESS MÉMOIRE ===")
            logging.info(f"Durée totale: {total_time:.2f}s")
            logging.info(f"Tests réussis: {len(successful_tests)}/{len(results)}")
            logging.info(f"Tests échoués: {len(failed_tests)}")

            if successful_tests:
                max_successful_size = max([int(k.split('_')[1]) for k in successful_tests])
                logging.info(f"Taille maximum traitée avec succès: {max_successful_size:,} lignes")

            results["summary"] = {
                "total_time_seconds": round(total_time, 2),
                "successful_tests": len(successful_tests),
                "failed_tests": len(failed_tests),
                "max_successful_size": max_successful_size if successful_tests else 0,
                "spark_version": spark.version,
                "total_tests": len(results) - 1  # -1 pour exclure ce summary
            }

            logging.info("=== FIN DU TEST DE STRESS MÉMOIRE ===")

            return results

        except Exception as e:
            logging.error(f"Erreur critique dans le test de stress mémoire: {e}")
            raise

        finally:
            Spark.close()

    @customTask
    @staticmethod
    def spark_stress_test_cpu(**kwargs) -> dict:
        """Test les limites CPU de Spark avec monitoring détaillé."""

        try:
            spark = Spark.get(**kwargs)

            logging.info("=== DÉBUT DU TEST DE STRESS CPU SPARK ===")
            logging.info(f"Configuration Spark: {spark.sparkContext.getConf().getAll()}")
            logging.info(f"Version Spark: {spark.version}")

            results = {}
            total_start_time = time.time()

            # Tests progressifs avec différentes charges CPU
            test_configs = [
                {"name": "light", "rows": 100_000, "iterations": 5, "description": "Charge légère"},
                {"name": "medium", "rows": 500_000, "iterations": 10, "description": "Charge moyenne"},
                {"name": "heavy", "rows": 1_000_000, "iterations": 15, "description": "Charge lourde"},
                {"name": "extreme", "rows": 2_000_000, "iterations": 20, "description": "Charge extrême"},
                {"name": "insane", "rows": 5_000_000, "iterations": 25, "description": "Charge insensée"},
                {"name": "ultimate", "rows": 10_000_000, "iterations": 30, "description": "Charge ultime"},
            ]

            for i, config in enumerate(test_configs, 1):
                test_name = config["name"]
                rows = config["rows"]
                iterations = config["iterations"]
                description = config["description"]

                logging.info(f"--- TEST {i}/{len(test_configs)}: {test_name.upper()} ({description}) ---")
                logging.info(f"Lignes: {rows:,}, Itérations de calcul: {iterations}")

                test_start_time = time.time()

                try:
                    # Créer le DataFrame de base
                    logging.info(f"Création du DataFrame de {rows:,} lignes...")
                    df_start = time.time()
                    df = spark.range(rows)
                    df_creation_time = time.time() - df_start

                    logging.info(f"DataFrame créé en {df_creation_time:.2f}s")

                    # Phase 1: Calculs mathématiques intensifs
                    logging.info("Phase 1: Calculs mathématiques intensifs...")
                    math_start = time.time()

                    for j in range(iterations):
                        if j < 5:
                            # Calculs trigonométriques
                            df = df.withColumn(f"trig_{j}",
                                F.sin(F.col("id") / 1000.0) * F.cos(F.col("id") / 1000.0) * F.tan(F.col("id") / 10000.0))
                        elif j < 10:
                            # Calculs exponentiels et logarithmiques
                            df = df.withColumn(f"exp_{j}",
                                F.exp(F.col("id") / 1000000.0) + F.log(F.col("id") + 1))
                        elif j < 15:
                            # Calculs de puissance et racines
                            df = df.withColumn(f"power_{j}",
                                F.pow(F.col("id"), 0.5) + F.sqrt(F.col("id") + 1))
                        else:
                            # Combinaisons complexes
                            df = df.withColumn(f"complex_{j}",
                                F.sqrt(F.sin(F.col("id") / 1000.0)) + F.log(F.cos(F.col("id") / 1000.0) + 2))

                    math_time = time.time() - math_start
                    logging.info(f"Phase 1 terminée en {math_time:.2f}s")

                    # Phase 2: Agrégations intensives
                    logging.info("Phase 2: Agrégations intensives...")
                    agg_start = time.time()

                    # Première série d'agrégations
                    agg_cols = [col for col in df.columns if col != "id"][:min(10, len(df.columns)-1)]

                    agg_exprs = []
                    for col in agg_cols:
                        agg_exprs.extend([
                            F.sum(col).alias(f"sum_{col}"),
                            F.avg(col).alias(f"avg_{col}"),
                            F.max(col).alias(f"max_{col}"),
                            F.min(col).alias(f"min_{col}"),
                            F.stddev(col).alias(f"stddev_{col}")
                        ])

                    agg_result = df.agg(*agg_exprs).collect()[0]
                    agg_time = time.time() - agg_start

                    logging.info(f"Phase 2 terminée en {agg_time:.2f}s")

                    # Phase 3: Opérations de fenêtrage (Window functions)
                    if rows <= 1_000_000:  # Limiter pour éviter les explosions mémoire
                        logging.info("Phase 3: Fonctions de fenêtrage...")
                        window_start = time.time()

                        from pyspark.sql.window import Window

                        # Créer des fenêtres partitionnées
                        window_spec = Window.partitionBy(F.col("id") % 100).orderBy("id")

                        df_windowed = df.withColumn("row_number", F.row_number().over(window_spec))
                        df_windowed = df_windowed.withColumn("rank", F.rank().over(window_spec))

                        # Calculer quelques statistiques sur les fenêtres
                        window_count = df_windowed.count()
                        window_time = time.time() - window_start

                        logging.info(f"Phase 3 terminée: {window_count:,} lignes en {window_time:.2f}s")
                    else:
                        window_time = 0
                        window_count = 0
                        logging.info("Phase 3 ignorée (taille trop importante)")

                    # Phase 4: Count final avec cache
                    logging.info("Phase 4: Count final avec mise en cache...")
                    cache_start = time.time()

                    df_cached = df.cache()
                    final_count = df_cached.count()
                    partitions = df_cached.rdd.getNumPartitions()

                    cache_time = time.time() - cache_start
                    logging.info(f"Phase 4 terminée: {final_count:,} lignes, {partitions} partitions en {cache_time:.2f}s")

                    # Nettoyage
                    df_cached.unpersist()

                    total_test_time = time.time() - test_start_time

                    # Calcul de quelques métriques de performance
                    throughput = final_count / total_test_time if total_test_time > 0 else 0
                    cpu_efficiency = (math_time + agg_time) / total_test_time * 100 if total_test_time > 0 else 0

                    results[test_name] = {
                        "status": "success",
                        "config": {
                            "rows": rows,
                            "iterations": iterations,
                            "description": description
                        },
                        "timings": {
                            "df_creation_seconds": round(df_creation_time, 2),
                            "math_phase_seconds": round(math_time, 2),
                            "aggregation_phase_seconds": round(agg_time, 2),
                            "window_phase_seconds": round(window_time, 2),
                            "cache_phase_seconds": round(cache_time, 2),
                            "total_test_seconds": round(total_test_time, 2)
                        },
                        "results": {
                            "final_count": final_count,
                            "partitions": partitions,
                            "window_count": window_count,
                            "columns_created": len(df.columns) - 1  # -1 pour exclure la colonne id
                        },
                        "performance": {
                            "throughput_rows_per_second": round(throughput, 2),
                            "cpu_efficiency_percent": round(cpu_efficiency, 2)
                        },
                        "sample_aggregation_results": {
                            key: float(value) if value is not None else 0.0
                            for key, value in dict(agg_result.asDict()).items()
                            if 'sum_' in key or 'avg_' in key
                        }
                    }

                    logging.info(f"✅ Test {test_name} réussi en {total_test_time:.2f}s")
                    logging.info(f"   Débit: {throughput:,.0f} lignes/sec")
                    logging.info(f"   Efficacité CPU: {cpu_efficiency:.1f}%")
                    logging.info(f"   Colonnes créées: {len(df.columns) - 1}")

                except Exception as e:
                    test_time = time.time() - test_start_time
                    error_msg = str(e)

                    logging.error(f"❌ Test {test_name} échoué après {test_time:.2f}s")
                    logging.error(f"   Erreur: {error_msg}")

                    results[test_name] = {
                        "status": "failed",
                        "config": {
                            "rows": rows,
                            "iterations": iterations,
                            "description": description
                        },
                        "error": error_msg,
                        "test_time_seconds": round(test_time, 2)
                    }

                    # Arrêter si erreur critique
                    if "OutOfMemoryError" in error_msg or "Cannot allocate" in error_msg:
                        logging.warning("Limite des ressources atteinte, arrêt des tests suivants")
                        break

            total_time = time.time() - total_start_time

            # Résumé final
            successful_tests = [k for k, v in results.items() if v["status"] == "success"]
            failed_tests = [k for k, v in results.items() if v["status"] == "failed"]

            logging.info("=== RÉSUMÉ DU TEST DE STRESS CPU ===")
            logging.info(f"Durée totale: {total_time:.2f}s")
            logging.info(f"Tests réussis: {len(successful_tests)}/{len(results)}")
            logging.info(f"Tests échoués: {len(failed_tests)}")

            if successful_tests:
                # Calcul du test le plus performant
                best_test = max(
                    [results[test] for test in successful_tests],
                    key=lambda x: x["performance"]["throughput_rows_per_second"]
                )
                logging.info(f"Meilleur débit: {best_test['performance']['throughput_rows_per_second']:,.0f} lignes/sec")

            # Ajout du résumé
            results["summary"] = {
                "total_time_seconds": round(total_time, 2),
                "successful_tests": len(successful_tests),
                "failed_tests": len(failed_tests),
                "spark_version": spark.version,
                "best_throughput": best_test["performance"]["throughput_rows_per_second"] if successful_tests else 0
            }

            logging.info("=== FIN DU TEST DE STRESS CPU ===")

            return results

        except Exception as e:
            logging.error(f"Erreur critique dans le test de stress CPU: {e}")
            raise

        finally:
            Spark.close()