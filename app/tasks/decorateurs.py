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
    
def marquezTask(func=None, *, description=None, task_id=None):
    """Décorateur qui intègre automatiquement Marquez avec gestion des événements.
    
    Args:
        func (function): La fonction de la tâche à décorer.
        description (str, optional): Description personnalisée pour Marquez. Si None, utilise le nom de la fonction.
        task_id (str, optional): ID de la tâche.
    
    Returns:
        function: La fonction décorée avec Marquez intégré.
    """
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(**kwargs):

            # Vérifier que nous sommes dans un contexte d'exécution de tâche
            if 'run_id' not in kwargs or 'dag' not in kwargs:
                # Si nous ne sommes pas dans un contexte d'exécution, 
                # exécuter la fonction sans Marquez
                return func(**kwargs)

            dag_id = kwargs.get('dag').dag_id
            run_id = kwargs.get('run_id', None)

            # Récupérer le task_id depuis plusieurs sources possibles
            job_name = None
            if task_id:
                # task_id fourni en paramètre du décorateur
                job_name = task_id
            elif 'task' in kwargs and hasattr(kwargs['task'], 'task_id'):
                # task_id depuis l'objet task dans le contexte
                job_name = kwargs['task'].task_id
            elif 'task_id' in kwargs:
                # task_id directement dans kwargs (fallback)
                job_name = kwargs['task_id']
            else:
                # Dernière tentative : utiliser le nom de la fonction
                job_name = func.__name__

            if not job_name:
                raise AirflowFailException("Le nom du job (provenant de task_id) ne peut pas être déterminé pour Marquez.")
            if not run_id:
                raise AirflowFailException("Le run_id (provenant de run_id) ne peut pas être déterminé pour Marquez.")
            if not dag_id:
                raise AirflowFailException("Le dag_id (provenant de dag.dag_id) ne peut pas être déterminé pour Marquez.")
            
            # Initialiser Marquez automatiquement
            marquez_description = description or f"Exécution de {func.__name__}"
            
            # Créer un job_name unique pour éviter la persistance des métadonnées entre les runs
            # Ceci force Marquez à traiter chaque exécution comme un job distinct
            unique_job_name = f"{job_name}_{run_id.replace('manual__', '').replace(':', '_').replace('+', '_')}"
            
            marquez = manager.Marquez(
                run_id=run_id,
                job_namespace=dag_id,
                job_name=unique_job_name,
                description=marquez_description,
            )
            
            # S'assurer que les inputs/outputs sont vides
            marquez.set_metadata_sql(
                query="",
                inputs=[],
                outputs=[],
            )
            
            # Maintenant envoyer l'événement START avec les bonnes métadonnées
            marquez.event(manager.MarquezEventType.START)
            
            # Ajouter l'instance marquez aux kwargs pour que la fonction puisse l'utiliser
            kwargs['marquez'] = marquez
            
            try:

                # Exécution de la fonction
                result = func(**kwargs)
                
                # Marquer la tâche comme complète
                marquez.event(manager.MarquezEventType.COMPLETE)
                
                return result
                
            except Exception as e:
                # Marquer la tâche comme échouée
                marquez.event(manager.MarquezEventType.FAIL)
                raise

        return wrapper

    # Gérer l'utilisation avec ou sans parenthèses
    if func is None:
        return decorator
    else:
        return decorator(func)