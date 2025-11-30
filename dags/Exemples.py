import sys
import os
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import chain, TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.tasks.exemple as exemple

DAG_ID = "exemple_dag"
DESCRIPTION = "Ce DAG extrait des données depuis SQL Server d'A2PO et BigQuery de GCP, puis les transmet sous forme de liste dans le XCom."
OBJECTIF = "Valider les étapes d'extraction des données depuis BigQuery et SQL Server."

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
    schedule="00 1 * * 1-7",  # Fréquence d'exécution (CRON ou timedelta)
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

    task_basic_hello = exemple.Basic.hello(
        task_id="task_basic_hello",
        name="Airflow User",
    )

    task_basic_xcom = exemple.Basic.xcom(
        task_id="task_basic_xcom",
        var_test="Test de la tâche basic",
    )

    task_spark = exemple.Basic.spark_df(
        task_id="task_spark_df",
    )

    with TaskGroup("groupe_call_API") as groupe_call_api:

        call_api_raw = exemple.Basic.call_API(
            task_id="call_api_raw",
            http_conn_id="API_jsonplaceholder",
            endpoint="/posts/1",
            method="GET",
            data=None,
            headers={"Accept": "application/json"},
            log_response=True,
        )

        insert_warehouse = exemple.Basic.insert_warehouse(
            task_id="insert_warehouse",
            df=pd.DataFrame([
                {"id": 1, "titre": "Premier titre", "contenu": "Premier contenu"},
                {"id": 2, "titre": "Deuxième titre", "contenu": "Deuxième contenu"},
                {"id": 3, "titre": "Troisième titre", "contenu": "Troisième contenu"},
                {"id": 4, "titre": "dqdsqdsqdsq", "contenu": "dsqdq contenu"}
            ]),  # DataFrame pandas
            table_name="exemple_table",
        )

        chain(call_api_raw, insert_warehouse,)

    chain(
        [task_basic_hello, task_basic_xcom,task_spark,],
        groupe_call_api,
    )
