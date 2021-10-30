import pytest
from bigquery_utils import bigquery


def test_client_creation():
    bq = bigquery.BigQueryClient()
    assert type(bq) == bigquery.BigQueryClient

    # Check list of implemented methods
    assert all(method in dir(bq) for method in ["client", "archive"])
