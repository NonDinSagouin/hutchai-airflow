#!/usr/bin/env python3
"""
Test script pour vérifier la création d'événements OpenLineage dans Marquez
"""

import requests
import json
from datetime import datetime

def create_test_lineage_job():
    """
    Crée un job de test avec des datasets pour vérifier la visibilité dans Marquez
    """
    
    marquez_url = "http://localhost:5000"
    
    # Définir les datasets d'entrée et de sortie
    input_dataset = {
        "namespace": "hutchai_lol",
        "name": "lol_fact_puuid_to_process",
        "facets": {
            "schema": {
                "_producer": "test-script",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/SchemaDatasetFacet.json",
                "fields": [
                    {"name": "puuid", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"}
                ]
            }
        }
    }
    
    output_dataset = {
        "namespace": "hutchai_lol", 
        "name": "lol_raw_match_datas_ids",
        "facets": {
            "schema": {
                "_producer": "test-script",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/SchemaDatasetFacet.json",
                "fields": [
                    {"name": "match_id", "type": "VARCHAR"},
                    {"name": "puuid", "type": "VARCHAR"},
                    {"name": "created_at", "type": "TIMESTAMP"}
                ]
            }
        }
    }
    
    # Créer l'événement OpenLineage
    openlineage_event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {
            "runId": "test-run-" + str(int(datetime.utcnow().timestamp()))
        },
        "job": {
            "namespace": "hutchai_lol",
            "name": "test_lineage_job",
            "facets": {
                "documentation": {
                    "_producer": "test-script",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json",
                    "description": "Test job pour vérifier la visibilité des datasets dans Marquez"
                }
            }
        },
        "inputs": [input_dataset],
        "outputs": [output_dataset],
        "producer": "test-script"
    }
    
    try:
        print("🚀 Envoi de l'événement OpenLineage à Marquez...")
        response = requests.post(
            f"{marquez_url}/api/v1/lineage",
            headers={"Content-Type": "application/json"},
            data=json.dumps(openlineage_event)
        )
        
        if response.status_code in [200, 201]:
            print(f"✅ Événement envoyé avec succès! (Status: {response.status_code})")
            return True
        else:
            print(f"❌ Erreur lors de l'envoi: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def check_datasets_and_jobs():
    """Vérifie les datasets et jobs dans Marquez"""
    marquez_url = "http://localhost:5000"
    
    try:
        print("\n📊 Vérification des datasets...")
        datasets_response = requests.get(f"{marquez_url}/api/v1/namespaces/hutchai_lol/datasets")
        
        if datasets_response.status_code == 200:
            datasets_data = datasets_response.json()
            print(f"   Datasets trouvés: {datasets_data.get('totalCount', 0)}")
            
            for dataset in datasets_data.get('datasets', []):
                name = dataset.get('name', 'Unknown')
                updated = dataset.get('updatedAt', 'Never')
                print(f"   - {name} (mis à jour: {updated})")
        
        print("\n🏭 Vérification des jobs...")
        jobs_response = requests.get(f"{marquez_url}/api/v1/namespaces/hutchai_lol/jobs")
        
        if jobs_response.status_code == 200:
            jobs_data = jobs_response.json()
            print(f"   Jobs trouvés: {jobs_data.get('totalCount', 0)}")
            
            for job in jobs_data.get('jobs', []):
                name = job.get('name', 'Unknown')
                job_type = job.get('type', 'Unknown')
                updated = job.get('updatedAt', 'Never')
                print(f"   - {name} ({job_type}) - mis à jour: {updated}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")
        return False

if __name__ == "__main__":
    print("=== Test de création d'événement OpenLineage ===")
    
    # Vérifier l'état initial
    print("\n📋 État initial:")
    check_datasets_and_jobs()
    
    # Créer le job de test
    if create_test_lineage_job():
        print("\n📋 État après création:")
        check_datasets_and_jobs()
        
        print("\n🌐 Vérifiez l'interface Marquez:")
        print("   URL: http://localhost:3001")
        print("   Vous devriez voir les datasets et leur lineage!")
    
    print("\n✨ Test terminé!")
