#!/usr/bin/env python3
"""
Script pour nettoyer les exécutions (runs) des DAGs Airflow.
Usage:
    python clear_dag_runs.py <dag_id>    # Nettoie les runs d'un DAG spécifique
    python clear_dag_runs.py all         # Nettoie les runs de tous les DAGs
"""

import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import func

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@provide_session
def clear_dag_runs(dag_id=None, session=None):
    """
    Supprime les runs d'un DAG spécifique ou de tous les DAGs.

    Args:
        dag_id (str): ID du DAG à nettoyer. Si None ou 'all', nettoie tous les DAGs.
        session: Session SQLAlchemy
    """
    try:
        if dag_id and dag_id.lower() != 'all':
            # Nettoyer un DAG spécifique
            logger.info(f"Nettoyage des runs pour le DAG: {dag_id}")

            # Compter les runs avant suppression
            run_count = session.query(DagRun).filter(DagRun.dag_id == dag_id).count()
            task_count = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count()

            if run_count == 0:
                logger.warning(f"Aucun run trouvé pour le DAG: {dag_id}")
                return

            logger.info(f"Trouvé {run_count} runs et {task_count} instances de tâches pour le DAG {dag_id}")

            # Supprimer les instances de tâches d'abord (contraintes de clé étrangère)
            deleted_tasks = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
            logger.info(f"Supprimé {deleted_tasks} instances de tâches")

            # Supprimer les runs de DAG
            deleted_runs = session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
            logger.info(f"Supprimé {deleted_runs} runs de DAG")

        else:
            # Nettoyer tous les DAGs
            logger.info("Nettoyage des runs pour TOUS les DAGs")

            # Compter avant suppression
            total_runs = session.query(DagRun).count()
            total_tasks = session.query(TaskInstance).count()

            if total_runs == 0:
                logger.warning("Aucun run trouvé dans la base de données")
                return

            logger.info(f"Trouvé {total_runs} runs et {total_tasks} instances de tâches au total")

            # Confirmation supplémentaire pour la suppression totale
            logger.warning("ATTENTION: Vous êtes sur le point de supprimer TOUS les runs de TOUS les DAGs!")

            # Supprimer toutes les instances de tâches
            deleted_tasks = session.query(TaskInstance).delete()
            logger.info(f"Supprimé {deleted_tasks} instances de tâches")

            # Supprimer tous les runs de DAG
            deleted_runs = session.query(DagRun).delete()
            logger.info(f"Supprimé {deleted_runs} runs de DAG")

        # Valider les modifications
        session.commit()
        logger.info("Nettoyage terminé avec succès!")

    except Exception as e:
        logger.error(f"Erreur lors du nettoyage: {str(e)}")
        session.rollback()
        raise


@provide_session
def get_dag_stats(dag_id=None, session=None):
    """
    Affiche les statistiques des DAGs avant suppression.

    Args:
        dag_id (str): ID du DAG. Si None, affiche pour tous les DAGs.
        session: Session SQLAlchemy
    """
    try:
        if dag_id and dag_id.lower() != 'all':
            # Stats pour un DAG spécifique
            runs = session.query(DagRun).filter(DagRun.dag_id == dag_id)
            tasks = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        else:
            # Stats pour tous les DAGs
            runs = session.query(DagRun)
            tasks = session.query(TaskInstance)

        run_count = runs.count()
        task_count = tasks.count()

        if run_count > 0:
            # Stats par état
            run_states = session.query(
                DagRun.state,
                func.count(DagRun.state)
            ).filter(
                DagRun.dag_id == dag_id if dag_id and dag_id.lower() != 'all' else True
            ).group_by(DagRun.state).all()

            logger.info("=== STATISTIQUES AVANT SUPPRESSION ===")
            logger.info(f"Total runs: {run_count}")
            logger.info(f"Total instances de tâches: {task_count}")
            logger.info("Répartition par état:")
            for state, count in run_states:
                logger.info(f"  - {state}: {count}")
        else:
            logger.info("Aucun run trouvé.")

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des statistiques: {str(e)}")


def main():
    """Fonction principale du script."""
    if len(sys.argv) != 2:
        logger.error("Usage: python clear_dag_runs.py <dag_id|all>")
        logger.error("Exemples:")
        logger.error("  python clear_dag_runs.py my_dag_id")
        logger.error("  python clear_dag_runs.py all")
        sys.exit(1)

    dag_id = sys.argv[1].strip()

    if not dag_id:
        logger.error("Le nom du DAG ne peut pas être vide")
        sys.exit(1)

    try:
        # Afficher les statistiques avant suppression
        get_dag_stats(dag_id)

        # Effectuer le nettoyage
        clear_dag_runs(dag_id)

        # Afficher les statistiques après suppression
        logger.info("=== STATISTIQUES APRÈS SUPPRESSION ===")
        get_dag_stats(dag_id)

    except Exception as e:
        logger.error(f"Échec du nettoyage: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
