from bigquery_utils.config import DEFAULT_BIGQUERY_CONFIG
from bigquery_utils.utils import Response
from google.cloud import bigquery
from google.cloud import exceptions
import logging
import dataclasses


@dataclasses.dataclass
class BigQueryClientResponse:
    code: int = 0
    response: Response = None
    steps: [Response] = dataclasses.field(default_factory=list)
    target: bigquery.TableReference = None
    destination: bigquery.DatasetReference = None

    def add_step(self, step: Response) -> None:
        if step.code != 0:
            self.code = 1
        self.steps.append(step)


class BigQueryClient:
    def __init__(self) -> None:
        self.client = None

    def _create_client(
        self, remake: bool = False, defaults: dict = DEFAULT_BIGQUERY_CONFIG
    ) -> None:
        if self.client is None or remake:
            logging.debug("Initialising BigQuery client")
            self.client = bigquery.Client(
                project=defaults.get("project"),
                credentials=defaults.get("credentials"),
                location=defaults.get("location"),
                default_query_job_config=defaults.get("query_job_config"),
            )
            logging.debug("BigQuery client initalised successfully")

    def archive(
        self,
        target: bigquery.TableReference,
        destination: bigquery.DatasetReference = None,
        location: str = None,
        overwrite: bool = False,
        confirmation: bool = True,
    ) -> BigQueryClientResponse:
        """
        Archive a table or view

        Creates an archive dataset if one does not already exist
        Copies the target to the archive dataset
        Deletes the target

        If dataset creation or target copy fails, the target will not be
        deleted. However, the process it not atomic; if a dataset has been
        created, or a table or view copied, they will not be deleted if a
        later stage fails.

        This should be updated to take advantage of BigQuery's multi-statement
        transactions as documented here:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/transactions
        It currently doesn't support DML statements, but that may change.

        Parameters
        ----------
        target: bigquery.TableReference
            the table or view to be archived
        destination: bigquery.DatasetReference, optional
            the dataset in which to store the archived table or view
            default: dataset named `archive` in same project as target
        location: str, optional
            where to run the job
            default: picked up from config
        overwrite: bool, optional
            should existing tables and views in the destination be overridden
            default: False
        confirmation: bool, optional
            should confirmation be required before deletion
            default: True
        """

        logging.debug("Running BigQuery archive function")

        res = BigQueryClientResponse()

        # ensure client is created
        self._create_client()

        # create archive dataset
        try:
            if destination is None:
                logging.debug("No destination provided, using default")
                destination = bigquery.DatasetReference(target.project, "archive")
            res.destination = destination
            logging.debug(f"Attempting to create archive dataset {destination}")
            self.client.create_dataset(destination)
            logging.info(f"Successfully created archive dataset {destination}")
            res.add_step(Response("dataset", 0))
        except exceptions.Conflict:
            logging.debug(f"Dataset {destination} already exists")
            res.add_step(Response("dataset", 0, "Dataset already exists"))

        # copy table / view
        res.target = target
        tab = self.client.get_table(target)
        if tab.table_type == "TABLE":
            logging.debug("Creating copy job for table")
            job_config = bigquery.CopyJobConfig(
                write_disposition="WRITE_TRUNCATE" if overwrite else "WRITE_EMPTY"
            )
            job = self.client.copy_table(
                target,
                bigquery.TableReference(destination, target.table_id),
                location=location if location else None,
                job_config=job_config,
            )

            if job.done and not job.errors:
                logging.debug("Copy of table completed")
                res.add_step(Response("copy", 0))
            else:
                logging.debug(f"Errors in copy: {job.errors}")
                res.add_step(Response("copy", 1, job.errors))
        elif tab.table_type == "VIEW":
            logging.debug("Creating copy of view")
            view = bigquery.Table(bigquery.TableReference(destination, target.table_id))
            view.view_query = tab.view_query
            view = self.client.create_table(view)
            logging.debug("Copy of view completed")
            res.add_step(Response("copy", 0))
        else:
            raise ValueError(f"Unsupported table type: {tab.table_type}")

        if confirmation:
            confirm = input(f"Copy complete. Confirm deletion of {target} y/n: ")
        else:
            confirm = "y"

        if confirm.lower() in ["y", "yes"]:
            logging.debug(f"Deleting {target}")
            self.client.delete_table(target)
            res.add_step(Response("delete", 0))
        else:
            logging.debug("Aborting deletion due to user input")
            res.add_step(Response("delete", 1))

        return res
