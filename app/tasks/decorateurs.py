import functools
from airflow.sdk import task

from app.helper.logging_title import logging_title

def customTask(func):
    """Décorateur pour logger le début et la fin d'une tâche.

    Args:
        func (function): La fonction de la tâche à décorer.
        
    Returns:
        function: La fonction décorée avec logging.
    """

    @functools.wraps(func)
    def wrapper(**kwargs):
        # Extraire task_id des kwargs s'il existe
        task_id = kwargs.pop('task_id', None)

        # Logging de début
        logging_title("🚀 Démarrage de la tâche", lvl=1)

        # Exécution de la fonction
        result = func(**kwargs)

        # Logging de fin avec succès
        logging_title("🔥 Fin de la tâche", lvl=1)

        return result

    # Appliquer le décorateur @task avec task_id si fourni
    def task_wrapper(**kwargs):
        task_id = kwargs.get('task_id', None)
        if task_id:
            return task(task_id=task_id)(wrapper)(**kwargs)
        else:
            return task(wrapper)(**kwargs)
    
    return task_wrapper