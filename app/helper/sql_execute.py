from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, engine

def sql_execute(engine : engine, query : str) -> bool:
    """Exécute une requête SQL en utilisant SQLAlchemy.

    Args:
        engine (engine): L'objet moteur SQLAlchemy pour se connecter à la base de données.
        query (str): La requête SQL à exécuter.

    Returns:
        bool: Retourne True si la requête a été exécutée avec succès, sinon False.
    """

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        session.execute(text(query))
        session.commit()

    except Exception as e:
        session.rollback()
        raise RuntimeError(f"❌ Erreur lors de l'éxecution de la requête : {e}")

    finally:
        session.close()

    return True
