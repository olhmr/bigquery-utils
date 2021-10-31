from bigquery_utils.utils import parse_bigquery_reference
import argparse
from bigquery_utils import bigquery_client
import logging

# TODO: add function for iterating through a list of tables in a dataset
# TODO: add generic validation function for project, dataset, and table ids
# TODO: add confirmation dialogue
# TODO: add exception handling
# TODO: move utility functions to separate file
# TODO: add tests


if __name__ == "__main__":
    logging.debug("Creating parsers")
    parser = argparse.ArgumentParser(description="BigQuery Utility Functions")
    subparsers = parser.add_subparsers(dest="command")

    # archive functionality
    archive = subparsers.add_parser("archive")
    archive.add_argument("target", help="Table to archive", type=str, action="store")
    archive.add_argument(
        "destination", help="Dataset to archive to", type=str, action="store"
    )
    archive.add_argument(
        "location", help="Location to run job in", nargs="?", type=str, action="store"
    )
    archive.add_argument(
        "overwrite",
        help="Flag indicating whether exising table should be overwritten, default false",
        nargs="?",
        type=bool,
        default=False,
        action="store",
    )

    logging.debug("Parsing arguments")
    args = parser.parse_args()

    if args.command == "archive":
        logging.debug("Parsing archive command")
        target = parse_bigquery_reference(ref_type="table", ref=args.target)
        destination = parse_bigquery_reference(ref_type="dataset", ref=args.destination)
        if "location" in args:
            location = args.location
        if "overwrite" in args:
            overwrite = args.overwrite

        logging.debug("Running archive command")
        bq = bigquery_client.BigQueryClient()
        try:
            res = bq.archive(
                target=target,
                destination=destination,
                location=location,
                overwrite=overwrite,
            )
            if res.code == 0:
                print(f"Archived {target} in {destination} successfully")
            else:
                print(
                    f"Error encounterd while archiving: {[step for step in res.steps if step.code != 0]}"
                )
        except Exception as err:
            print(f"Unexpected error: {err}")
