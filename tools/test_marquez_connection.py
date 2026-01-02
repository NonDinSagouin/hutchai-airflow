#!/usr/bin/env python3

import sys
import os

# Ajouter le répertoire app au PYTHONPATH
sys.path.append('/opt/airflow/app')

from datetime import datetime, timezone
import uuid

# Importer directement depuis le fichier
sys.path.insert(0, '/opt/airflow/app/manager')
from Marquez import Marquez

def test_marquez_connection():
    """Test de connexion à Marquez depuis un conteneur Airflow"""
    
    print(f"🔗 URL Marquez configurée: {Marquez.URL}")
    
    try:
        # Créer un événement de test
        test_event = Marquez.event(
            event_type="START",
            run_id=str(uuid.uuid4()),
            event_time=datetime.now(timezone.utc).isoformat(),
            job_namespace="test",
            job_name="test_connection",
            inputs=[],
            outputs=[],
            query="SELECT 1 as test",
            description="Test de connexion à Marquez"
        )
        
        if test_event:
            print("✅ Connexion à Marquez réussie!")
            return True
        else:
            print("❌ Échec de l'envoi à Marquez")
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors du test de connexion: {e}")
        return False

if __name__ == "__main__":
    success = test_marquez_connection()
    sys.exit(0 if success else 1)