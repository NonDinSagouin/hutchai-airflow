import functools
from airflow.sdk import task

from app.helper.logging_title import logging_title

def customTask(func):
    """Décorateur pour logger le début et la fin d'une tâche."""

    @functools.wraps(func)
    def wrapper(**kwargs):

        # Logging de début
        logging_title("🚀 Démarrage de la tâche", lvl=1)

        # Exécution de la fonction
        result = func(**kwargs)

        # Logging de fin avec succès
        logging_title("🔥 Fin de la tâche", lvl=1)

        return result

    return task(wrapper)