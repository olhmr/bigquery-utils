import dataclasses
import re
from typing import Literal
from google.cloud import bigquery


@dataclasses.dataclass
class Response:
    name: str
    code: int
    additional: any = None


def standardise_bigquery_ref(ref: str) -> str:
    if re.match("^[A-Za-z0-9-]:", ref):
        ref = re.sub("^[A-Za-z0-9-](:)", ".", ref)
    return ref


def parse_bigquery_reference(
    ref_type: Literal["table", "dataset"],
    ref: str,
) -> [bigquery.TableReference, bigquery.DatasetReference]:
    if ref_type == "table":
        ref = standardise_bigquery_ref(ref)
        [project, dataset, table] = ref.split(".")
        return bigquery.TableReference(
            bigquery.DatasetReference(project=project, dataset_id=dataset),
            table_id=table,
        )
    elif ref_type == "dataset":
        ref = standardise_bigquery_ref(ref)
        [project, dataset] = ref.split(".")
        return bigquery.DatasetReference(project=project, dataset_id=dataset)
    else:
        raise ValueError("Type must be table or dataset")
