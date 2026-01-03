import functools
import app.manager as manager

from airflow.sdk import task

from app.helper.logging_title import logging_title
from airflow.exceptions import AirflowFailException

def customTask(func=None, *, outlets=None):
    """Décorateur pour logger le début et la fin d'une tâche.

    Args:
        func (function): La fonction de la tâche à décorer.
        outlets (list, optional): Liste des assets de sortie.

    Returns:
        function: La fonction décorée avec logging.
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapper(**kwargs):
            # Extraire les paramètres spéciaux des kwargs
            task_id = kwargs.pop('task_id', None)
            task_outlets = kwargs.pop('outlets', outlets)

            # Logging de début
            logging_title("🚀 Démarrage de la tâche", lvl=1)

            # Exécution de la fonction
            result = func(**kwargs)

            # Logging de fin avec succès
            logging_title("🔥 Fin de la tâche", lvl=1)

            return result

        # Appliquer le décorateur @task avec task_id et outlets si fournis
        def task_wrapper(**kwargs):
            task_id = kwargs.get('task_id', None)
            task_outlets = kwargs.get('outlets', outlets)

            # Préparer les arguments pour le décorateur @task
            task_kwargs = {}
            if task_id:
                task_kwargs['task_id'] = task_id
            if task_outlets:
                task_kwargs['outlets'] = task_outlets

            return task(**task_kwargs)(wrapper)(**kwargs)

        return task_wrapper

    # Gérer l'utilisation avec ou sans parenthèses
    if func is None:
        return decorator
    else:
        return decorator(func)