from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, engine
import pandas

def sql_fetch_all(
    engine: engine,
    query: str,
    to_dataframe: bool = True
    ):
    """Exécute une requête SQL SELECT et retourne les résultats sous forme de dict ou de DataFrame.

    Args:
        engine (engine): L'objet moteur SQLAlchemy pour se connecter à la base de données.
        query (str): La requête SQL SELECT à exécuter.
        to_dataframe (bool, optional): Si True, retourne un DataFrame. Sinon, retourne une liste de dictionnaires.
    """

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        result = session.execute(text(query))
        columns = result.keys()
        rows = result.fetchall()

        if to_dataframe:
            return pandas.DataFrame(rows, columns=columns)
        else:
            return [dict(zip(columns, row)) for row in rows]

    except Exception as _:
        session.rollback()
        return pandas.DataFrame() if to_dataframe else []

    finally:
        session.close()
