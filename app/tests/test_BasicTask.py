import sys
import os
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import app.tasks.z_exemple as z_exemple


class TestBasicOperator(unittest.TestCase):

    def test_initialization(self):
        """ Vérifie que l'opérateur Basic s'initialise correctement """
        operator = z_exemple.Basic(var_toto="test_value", task_id="test_task")

        self.assertEqual(operator.task_id, "test_task")
        self.assertTrue(hasattr(operator, "_Basic__var_toto"))  # Vérifie l'attribut privé

    def test_execution(self,):  # Ajout du paramètre mock_is_production
        """ Vérifie que l'opérateur s'exécute correctement """
        operator = z_exemple.Basic(var_toto="test_value")

        result = operator.execute(context={})  # Simule l'exécution de la tâche

        self.assertEqual(result, "toto")  # Vérifie que la sortie est correcte

if __name__ == "__main__":
    unittest.main()