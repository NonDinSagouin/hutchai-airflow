import sys
import os

from datetime import datetime, timedelta

from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.z_exemple as z_exemple

DAG_ID = "z_Stress_test"
DESCRIPTION = "Réalisation de tests de stress sur une SparkSession Airflow."
OBJECTIF = "Ce DAG a pour objectif de réaliser des tests de stress sur une SparkSession Airflow en évaluant les performances en termes de mémoire et de CPU."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
}

# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=datetime(2025, 1, 1), # Date de début du DAG
    tags=["exemple",], # Liste de tags pour catégoriser le DAG dans l'UI
    catchup=False, # Exécution des tâches manquées (True ou False)
    max_active_runs=1,  # Limite à 1 exécutions actives en même temps
    dagrun_timeout=timedelta(minutes=5),  # Timeout pour chaque exécution du DAG
    description=DESCRIPTION,
    doc_md=f"""
        ## 🔹 Description
        {DESCRIPTION}

        ## 🔹 Objectif
        {OBJECTIF}
    """,
) as dag:

    task_spark_stress_test_memory = z_exemple.Stress_test.spark_stress_test_memory(
        task_id="task_spark_stress_test_memory",
    )

    task_spark_stress_test_cpu = z_exemple.Stress_test.spark_stress_test_cpu(
        task_id="task_spark_stress_test_cpu",
    )

    [task_spark_stress_test_memory, task_spark_stress_test_cpu]