import dataclasses
import re
from typing import Literal
from google.cloud import bigquery
import unicodedata


@dataclasses.dataclass
class Response:
    name: str
    code: int
    additional: any = None


def standardise_bigquery_ref(ref: str) -> str:
    if re.search(r"^[A-Za-z0-9-]+:", ref):
        ref = re.sub(r"(^[A-Za-z0-9-]+)(:)", r"\1.", ref)
        if not validate_standardised_bigquery_reference(ref)[0]:
            raise ValueError(f"BigQuery reference {ref} is not valid")
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


def validate_standardised_bigquery_reference(ref: str) -> [bool, any]:
    try:
        parts = ref.split(".")
        if len(parts) > 3:
            raise ValueError("BigQuery reference has too many periods")
        valid = []

        valid.append(re.match(r"^[A-Za-z0-9-]+$", parts[0]))
        valid.append(re.match(r"^[A-Za-z0-9_]+$", parts[1]))

        if len(parts) == 3:
            table_valid = False
            for char in parts[2]:
                category = unicodedata.category(char)
                if category[0] in ["L", "M", "N"] or category in ["Pc", "Pd", "Zs"]:
                    table_valid = True
            valid.append(table_valid)
        if all(valid):
            return True, valid
        else:
            return (False, valid)
    except Exception as err:
        return False, err
