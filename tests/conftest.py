import sys
from pathlib import Path

import pytest

AIRFLOW_HOME = Path(__file__).resolve().parents[1]
DAGS_FOLDER = AIRFLOW_HOME / "dags"

# Mêmes chemins d'import qu'à l'exécution dans Airflow (plugins/ et dags/ dans sys.path).
for folder in (AIRFLOW_HOME / "plugins", DAGS_FOLDER):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
