import sys
import os
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.z_exemple as z_exemple

DAG_ID = "z_exemple_dag"
DESCRIPTION = "Ce DAG sert d'exemple pour démontrer diverses fonctionnalités d'Airflow."
OBJECTIF = "Illustrer l'utilisation des tâches de base, des groupes de tâches, et des échanges de données via XCom dans un DAG Airflow."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # Attendre l'exécution précédente
    'retries': 0, # Nombre de tentatives avant l'échec d'une tâche
    'retry_delay': timedelta(seconds=5), # Temps entre chaque tentative
}

# Définition du DAG
with DAG(
    dag_id=DAG_ID, # Identifiant unique du DAG
    default_args=default_args, # Dictionnaire contenant les paramètres par défaut des tâches
    start_date=datetime(2025, 1, 1), # Date de début du DAG
    #schedule="00 1 * * 1-7",  # Fréquence d'exécution (CRON ou timedelta)
    tags=["exemple",], # Liste de tags pour catégoriser le DAG dans l'UI
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

    task_basic_hello = z_exemple.Basic.hello(
        task_id="task_basic_hello",
        name="Airflow User",
    )

    with TaskGroup("groupe_load") as groupe_load:

        task_xcom_put = z_exemple.Basic.xcom_put(
            task_id="task_xcom_put",
            var_test="toto_test",
        )

        task_spark = z_exemple.Basic.spark_df(
            task_id="task_spark_df",
        )

        chain(task_xcom_put, task_spark,)

    task_xcom_get = z_exemple.Basic.xcom_get(
        task_id="task_xcom_get",
        xcom_source="groupe_load.task_xcom_put",
    )
    task_xcom_get_spark = z_exemple.Basic.xcom_get(
        task_id="task_xcom_get_spark",
        xcom_source="groupe_load.task_spark_df",
    )

    chain(
        [task_basic_hello, groupe_load],
    )
    chain(
        groupe_load,
        [task_xcom_get, task_xcom_get_spark],
    )
