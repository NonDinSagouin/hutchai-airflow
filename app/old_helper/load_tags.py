from airflow import DAG

def load_tags(dag : DAG):
    """Ajoute les tags des tâches du DAG au DAG lui-même en évitant les doublons.

    Args:
        dag (DAG): L'objet DAG dont les tâches peuvent contenir des tags.
    """

    for task in dag.tasks:

        if not hasattr(task, 'TAGS') or not isinstance(task.TAGS, (list, set)):
            continue  # Ignore les tâches sans tags ou avec un format incorrect

        # Mettre à jour les tags en évitant les doublons
        dag.tags = list(set(dag.tags).union(task.TAGS))