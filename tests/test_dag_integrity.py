from airflow.models import DagBag


def test_dags_load_without_errors():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    assert "orders_pipeline" in dagbag.dags
    assert "orders_report" in dagbag.dags