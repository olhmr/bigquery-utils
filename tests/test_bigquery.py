from google.cloud import bigquery
from bigquery_utils import bigquery_client
from unittest.mock import MagicMock

bigquery.TableReference = MagicMock()
bigquery.DatasetReference = MagicMock()
bq = bigquery_client.BigQueryClient()
bq.client = MagicMock()


def test_class_creation():
    assert type(bq) == bigquery_client.BigQueryClient

    # Check list of implemented methods
    assert all(method in dir(bq) for method in ["_create_client", "archive"])


bq.client.create_dataset = MagicMock()
bq.client.copy_table = MagicMock()
bq.client.delete_table = MagicMock()


def test_archive_happy_path():
    bq.client.copy_table.return_value.done = True
    bq.client.copy_table.return_value.errors = False
    res = bq.archive(target=MagicMock(), destination=MagicMock())

    assert bq.client.create_dataset.called
    assert bq.client.copy_table.called
    assert bq.client.delete_table.called
    assert res.code == 0
    assert res.target is not None
    assert res.destination is not None


def test_archive_error_in_copy():
    bq.client.copy_table.return_value.done = True
    bq.client.copy_table.return_value.errors = True
    res = bq.archive(target=MagicMock(), destination=MagicMock())

    assert bq.client.create_dataset.called
    assert bq.client.copy_table.called
    assert res.code == 1
    assert res.target is not None
    assert res.destination is not None


def test_archive_copy_never_finishes():
    bq.client.copy_table.return_value.done = False
    bq.client.copy_table.return_value.errors = False
    res = bq.archive(target=MagicMock(), destination=MagicMock())

    assert bq.client.create_dataset.called
    assert bq.client.copy_table.called
    assert res.code == 1
    assert res.target is not None
    assert res.destination is not None


def test_archive_copy_never_finishes_but_error():
    bq.client.copy_table.return_value.done = False
    bq.client.copy_table.return_value.errors = True
    res = bq.archive(target=MagicMock(), destination=MagicMock())

    assert bq.client.create_dataset.called
    assert bq.client.copy_table.called
    assert res.code == 1
    assert res.target is not None
    assert res.destination is not None
