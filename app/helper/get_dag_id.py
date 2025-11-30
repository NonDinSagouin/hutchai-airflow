from app.helper.load_kwargs_informations import load_kwargs_informations

def get_dag_id(**kwargs):
    """
    Récupère l'ID du DAG à partir du contexte d'exécution.

    Args:
        **kwargs: Paramètres additionnels, notamment le contexte d'exécution.

    Returns:
        str: L'ID du DAG.
    """
    kwargs_info = load_kwargs_informations(**kwargs)
    dag_id = kwargs_info.get('dag_id')

    if not dag_id:
        raise ValueError("❌ Le DAG ID est introuvable dans le contexte.")

    return dag_id