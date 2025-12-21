import pandas as pd
import logging
import pendulum

import app.manager as manager

from typing import Any
from app.tasks.decorateurs import customTask

class Combine():

    @customTask
    @staticmethod
    def merge_xcoms(
        xcom_sources: list[str],
        **kwargs,
    ) -> Any:
        """ Permet de fusionner les entrées XCom de plusieurs tâches sources.

        Args:
            xcom_sources (list[str]): Liste des identifiants des tâches sources dont les XComs doivent être fusionnés.

        Returns:
            Any: Données fusionnées provenant des XComs des tâches sources.

        Example:
            merged_data = Combine.merge_xcoms(
                task_id="merge_task",
                xcom_sources=["task1", "task2", "task3"]
            )
            Les données fusionnées seront disponibles dans le XCom de la tâche "merge_task".
        """
        merged_data = pd.DataFrame()

        for source in xcom_sources[1:]:

            data_to_merge = manager.Xcom.get(
                xcom_source= source,
                **kwargs,
            )
        
            merged_data = pd.concat([merged_data, data_to_merge], ignore_index=True)

        return manager.Xcom.put(
            input=merged_data,
            **kwargs,
        )