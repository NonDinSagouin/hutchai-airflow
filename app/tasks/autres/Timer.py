import pandas as pd
import logging
import pendulum
import datetime as dt

import app.manager as manager

from app.tasks.decorateurs import customTask

class Timer():

    @customTask
    @staticmethod
    def wait(
        timer: dt.timedelta,
        **kwargs,
    ) -> list:
        """ Permet de faire une pause dans l'exécution d'une tâche pendant une durée spécifiée.

        Args:
            timer (dt.timedelta): Durée de la pause.

        Returns:
            None

        Example:
            Timer.wait(
                task_id="wait_task",
                timer=dt.timedelta(minutes=5)
            )
            La tâche "wait_task" fera une pause de 5 minutes avant de continuer son exécution.
        """
        logging.info(f"⏳ Attente de {timer} ...")
        pendulum.sleep(timer.total_seconds())
        logging.info("✅ Attente terminée.")

        return {
            "status": "completed",
            "waited_duration": str(timer),
        }