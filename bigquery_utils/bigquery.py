from bigquery_utils.config import DEFAULT_BIGQUERY_CONFIG
from google.cloud import bigquery
from google.cloud import exceptions
import logging


class BigQueryClient:
    def __init__(self, defaults: dict = DEFAULT_BIGQUERY_CONFIG):
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
        destination: bigquery.DatasetReference,
        location: str = None,
        override: bool = False,
    ):
        """
        Archive a table or view

        Creates an archive dataset if one does not already exist
        Copies the target to the archive dataset
        Deletes the target

        If dataset creation or target copy fails, the target will not be
        deleted. However, the process it not atomic; if a dataset has been
        created, or a table or view copied, they will remain in BigQuery.

        This should be updated to take advantage of BigQuery's multi-statement
        transactions as documented here:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/transactions
        It currently doesn't support DML statements, but that may change.

        Parameters
        ----------
        target: bigquery.TableReference
            the table or view to be archived
        destination: bigquery.DatasetReference
            the dataset in which to store the archived table or view
        location: str, optional
            where to run the job
            default: picked up from config
        override: bool, optional
            should existing tables and views in the destination be overridden
            default: False
        """

        logging.debug("Running BigQuery archive function")

        # create archive dataset
        try:
            logging.debug(f"Attempting to create archive dataset {destination}")
            self.client.create_dataset(destination)
            logging.info(f"Successfully created archive dataset {destination}")
        except exceptions.Conflict:
            logging.debug(f"Dataset {destination} already exists")

        # copy table reference
        logging.debug("Creating copy job")
        job_config = bigquery.CopyJobConfig(
            write_disposition="WRITE_TRUNCATE" if override else "WRITE_EMPTY"
        )
        job = self.client.copy_table(
            target,
            bigquery.TableReference(destination, target.table_id),
            location=location if location else self.location,
            job_config=job_config,
        )

        if job.done and not job.errors:
            logging.debug(f"Copy completed, deleting {target}")
            self.client.delete_table(target)
        else:
            logging.debug(f"Errors in copy: {job.errors}")
