from importlib.metadata import metadata
import requests
import json
import logging
import os

from datetime import datetime, timezone
from enum import Enum
from airflow.exceptions import AirflowFailException

class MarquezEventType(Enum):
    START = "START"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"

class Marquez:

    URL = os.getenv("MARQUEZ_URL", "http://marquez:5000")
    HEADERS = {'Content-Type': 'application/json'}

    PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow"
    SCHEMA_URL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet"
    DATASOURCE_URL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/DataSourceDatasetFacet"

    def __init__(
        self,
        run_id : str,
        job_namespace : str,
        job_name : str,
        description : str = "",
    ):
        """ Initialise une instance Marquez pour un job spécifique

        Args:
            run_id (str): Identifiant unique de l'exécution du job
            job_namespace (str): Namespace du job
            job_name (str): Nom du job
            description (str): Description du job

        Returns:
            None

        Example:
            >>> marquez = Marquez(
            ...    run_id="123e4567-e89b-12d3-a456-426614174000",
            ...    job_namespace="hutchai_lol",
            ...    job_name="ZZZ_lol_enrich_fact_matchs",
            ...    description="Job d'enrichissement des faits de matchs LOL"
            ... )
            Initialise une instance Marquez pour le job spécifié.
        """
        self.__run_id = run_id
        self.__job_namespace = job_namespace
        self.__job_name = job_name
        self.__description = description

        # Initialiser les appels SQL (si nécessaire)
        self.__query = ""
        self.__inputs = []
        self.__outputs = []

    def __send(
        self,
        event : dict
    ) -> bool:
        """ Envoie un événement OpenLineage à Marquez """
        print(json.dumps(event, indent=4))
        try:
            logging.info("🚀 Envoi de l'événement OpenLineage à Marquez...")
            response = requests.post(
                f"{Marquez.URL}/api/v1/lineage",
                headers=Marquez.HEADERS,
                data=json.dumps(event)
            )

            if response.status_code in [200, 201]:
                logging.info(f"✅ Événement envoyé avec succès! (Status: {response.status_code})")
                return True
            else:
                logging.error(f"❌ Erreur lors de l'envoi: {response.status_code}")
                logging.error(f"Response: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Exception lors de l'envoi de l'événement: {e}")
            return False

    def set_metadata_sql(
        self, 
        query : str,
        inputs : list,
        outputs : list,
    ):
        """ Met à jour les métadonnées de l'événement

        Args:
            query (str): Requête SQL associée au job
            inputs (dict): Dictionnaire représentant les datasets d'entrée
            outputs (dict): Dictionnaire représentant les datasets de sortie

        Example:
            >>> marquez.set_metadata_sql(
            ...    query="SELECT puuid, status FROM lol_fact_puuid_to_process WHERE status = 'pending'",
            ...    inputs=[
            ...        marquez.build_dataset(
            ...            namespace="hutchai_lol",
            ...            name="lol_fact_puuid_to_process",
            ...            fields=[
            ...                {"name": "puuid", "type": "VARCHAR"},
            ...                {"name": "status", "type": "VARCHAR"},
            ...            ]
            ...        )
            ...    ],
            ...    outputs=[
            ...        marquez.build_dataset(
            ...            namespace="hutchai_lol",
            ...            name="ZZZ_lol_fact_datas.lol_fact_match",
            ...            fields=[
            ...                {"name": "puuid", "type": "VARCHAR"},
            ...                {"name": "status", "type": "VARCHAR"},
            ...            ]
            ...        )
            ...    ]
            ... )
            Met à jour les métadonnées de l'événement avec les informations spécifiées.
        """
        self.__query = query
        self.__inputs = inputs
        self.__outputs = outputs  

    def event(
        self,
        event_type : MarquezEventType,
        event_time : str = None,
    ) -> bool:
        """ Génère et envoie un événement OpenLineage à Marquez

        Args:
            event_type (str): Type d'événement (START, COMPLETE, FAIL)
            event_time (str): Timestamp de l'événement au format ISO 8601. Si None, utilise l'heure actuelle.

        Returns:
            bool: True si l'événement a été envoyé avec succès, False sinon
        
        Example:
            >>> event = Marquez.generate_event(
            ...    event_type="START",
            ...    event_time="2024-10-01T12:00:00Z",
            ... )
            Retourne true si l'envoi a réussi, false sinon.
        """
        if event_type not in MarquezEventType:
            raise AirflowFailException(f"Type d'événement invalide: {event_type}. Doit être l'un de {[e.value for e in MarquezEventType]}")

        if event_time is None:
            event_time = datetime.now(timezone.utc).isoformat()

        # Construction des facets du job
        job_facets = {
            "documentation": {
                "_producer": self.PRODUCER,
                "_schemaURL": self.SCHEMA_URL,
                "description": self.__description,
            }
        }

        job_facets["sql"] = {
            "_producer": self.PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SqlJobFacet.json",
            "query": self.__query
        }

        event = {
            "eventType": event_type.value,
            "eventTime": event_time,
            "run": {
                "runId": self.__run_id
            },
            "job": {
                "namespace": self.__job_namespace,
                "name": self.__job_name,
                "facets": job_facets
            },
            "inputs": self.__inputs,
            "outputs": self.__outputs,
            "producer": self.PRODUCER
        }
    
        return self.__send(event)
    
    def build_dataset(
        self,
        namespace : str,
        name : str,
        fields : dict
    ) -> dict:
        """ Construit un dataset pour un événement OpenLineage
        
        Args:
            namespace (str): Namespace du dataset
            name (str): Nom du dataset
            fields (dict): Schéma des champs du dataset

        Returns:
            dict: Dictionnaire représentant le dataset
        
        Example:
            >>> input_dataset = Marquez.build_dataset(
            ...    namespace="hutchai_lol",
            ...    name="lol_fact_puuid_to_process",
            ...    fields=[
            ...        {"name": "puuid", "type": "VARCHAR"},
            ...        {"name": "status", "type": "VARCHAR"},
            ...    ]
            ... )
            Retourne un dictionnaire représentant le dataset.
        """
        return {
            "namespace": namespace,
            "name": name,
            "facets": {
                "dataSource": {
                    "_producer": Marquez.PRODUCER,
                    "_schemaURL": Marquez.DATASOURCE_URL,
                    "name": namespace,
                    "uri": f"warehouse://{namespace}"
                },
                "schema": {
                    "_producer": Marquez.PRODUCER,
                    "_schemaURL": Marquez.SCHEMA_URL,
                    "fields": fields
                }
            }
        }
            