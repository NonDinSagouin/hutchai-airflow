import logging
import json

from airflow.models import TaskInstance
from typing import Any

def load_kwargs_informations(add_context: bool = False, **kwargs) -> dict[str, Any]:
    """ Extrait les informations utiles des kwargs passés à une tâche Airflow.

    Args:
        add_context (bool, optional): Indique si l'objet `TaskInstance` doit être ajouté au dictionnaire d'informations. Par défaut, False.

    Returns:
        dict: Un dictionnaire contenant ces informations :
            - dag_run (str): L'objet `DagRun` associé à l'exécution actuelle.
            - dag_id (str): L'identifiant du DAG en cours d'exécution.
            - task_id (str): L'identifiant de la tâche en cours d'exécution.
            - execution_date (str): La date d'exécution actuelle sous forme de chaîne.
            - next_execution_date (str): La date de la prochaine exécution sous forme de chaîne.
            - prev_execution_date (str): La date de l'exécution précédente sous forme de chaîne.
            - logical_date (str): La logical date du DAG run au format yyyy-MM-dd.
            - run_id (str): L'identifiant de l'exécution actuelle.
            - conf (dict): La configuration associée à l'exécution, sous forme de dictionnaire.
            - task_instance_key_str (str): La clé unique de l'instance de tâche.
            - context (str) (optionnel): L'objet `TaskInstance` si `add_context` est activé.
    """
    # Récupération de la logical_date depuis dag_run
    logical_date_formatted = None
    dag_run = kwargs.get("dag_run")
    if dag_run and hasattr(dag_run, 'logical_date') and dag_run.logical_date:
        try:
            logical_date_formatted = dag_run.logical_date.strftime('%Y-%m-%d')
        except (AttributeError, ValueError) as e:
            logging.warning(f"⚠️ Erreur lors du formatage de la logical_date : {e}")
            logical_date_formatted = str(dag_run.logical_date)

    info = {
        "dag_run": kwargs.get("dag_run"),
        "dag_id": kwargs.get("dag_run").dag_id if kwargs.get("dag_run") else None,
        "task_id": kwargs.get("task").task_id if kwargs.get("task") else None,
        "execution_date": str(kwargs.get("execution_date")),
        "next_execution_date": str(kwargs.get("next_execution_date")),
        "prev_execution_date": str(kwargs.get("prev_execution_date")),
        "logical_date": logical_date_formatted,
        "run_id": kwargs.get("run_id"),
        "conf": kwargs.get("conf", {}),
        "task_instance_key_str": kwargs.get("task_instance_key_str"),
    }

    if add_context:

        info["context"] = kwargs.get("ti")
        if not isinstance(info["context"], TaskInstance):
            raise ValueError("❌ Le contexte est invalide. Impossible de récupérer XCom.")

    logging.info(f"✅ Infos utiles des kwargs de {info['dag_id']}")
    logging.debug(json.dumps(info, default=str, indent=2))

    return info
