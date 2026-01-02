import requests
import json
import logging
import os

class Marquez:

    URL = os.getenv("MARQUEZ_URL", "http://marquez:5000")
    HEADERS = {'Content-Type': 'application/json'}

    PRODUCER = "hutchai-airflow"
    SCHEMA_URL = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"

    @staticmethod
    def __send(
        event : dict
    ) -> bool:
        """ Envoie un événement OpenLineage à Marquez """
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
        
    @staticmethod
    def event(
        event_type : str,
        run_id : str,
        event_time : str,
        job_namespace : str,
        job_name : str,
        inputs : list,
        outputs : list,
        query : str = "",
        description : str = "",
    ) -> bool:
        """ Génère un événement OpenLineage 

        Args:
            event_type (str): Type d'événement (START, COMPLETE, FAIL)
            run_id (str): Identifiant unique de l'exécution
            job_namespace (str): Namespace du job
            job_name (str): Nom du job
            inputs (list): Liste des datasets d'entrée
            outputs (list): Liste des datasets de sortie
            query (str): Requête SQL associée au job
            description (str): Description du job

        Returns:
            bool: True si l'événement a été envoyé avec succès, False sinon
        
        Example:
            >>> event = Marquez.generate_event(
            ...    event_type="COMPLETE",
            ...    run_id="123e4567-e89b-12d3-a456-426614174000",
            ...    job_namespace="hutchai_lol",
            ...    job_name="ZZZ_lol_enrich_fact_matchs",
            ...    inputs=[input_dataset],
            ...    outputs=[output_dataset],
            ... )
            Retourne un dictionnaire représentant l'événement OpenLineage.
        """
        if event_type not in ["START", "COMPLETE", "FAIL"]:
            raise ValueError("event_type must be one of: START, COMPLETE, FAIL")

        event = {
            "eventType": event_type,
            "eventTime": event_time,
            "run": {
                "runId": run_id
            },
            "job": {
                "namespace": job_namespace,
                "name": job_name,
                "facets": {
                    "documentation": {
                        "_producer": Marquez.PRODUCER,
                        "_schemaURL": Marquez.SCHEMA_URL,
                        "description": description,
                    },
                },
                "sql": {
                    "_producer": Marquez.PRODUCER,
                    "_schemaURL": Marquez.SCHEMA_URL,
                    "query": query
                },
            },
            "inputs": inputs,
            "outputs": outputs,
            "producer": Marquez.PRODUCER
        }
    
        return Marquez.__send(event)
    
    @staticmethod
    def build_dataset(
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
                "schema": {
                    "_producer": Marquez.PRODUCER,
                    "_schemaURL": Marquez.SCHEMA_URL,
                    "fields": fields
                }
            }
        }
            