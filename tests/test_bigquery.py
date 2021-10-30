from google.cloud import bigquery
from bigquery_utils import bigquery_client
from unittest.mock import MagicMock

bigquery.TableReference = MagicMock()
bigquery.DatasetReference = MagicMock()
bq = bigquery_client.BigQueryClient()
bq.client = MagicMock()


def test_client_creation():
    assert type(bq) == bigquery_client.BigQueryClient

    # Check list of implemented methods
    assert all(method in dir(bq) for method in ["_get_client", "archive"])


def test_archive():
    # bq.client.create_dataset = MagicMock(return_value=3)
    bq.client.create_dataset = MagicMock()
    bq.client.copy_table = MagicMock()
    bq.client.delete_table = MagicMock()

    bq.archive(target=MagicMock(), destination=MagicMock())

    assert bq.client.create_dataset.called
    assert bq.client.copy_table.called
    assert bq.client.delete_table.called
