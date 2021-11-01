from bigquery_utils.utils import parse_bigquery_reference
import argparse
from bigquery_utils import bigquery_client
import logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    # log level options
    parser.add_argument(
        "--debug",
        "-d",
        help="Set log level to DEBUG",
        action="store_const",
        dest="log_level",
        const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Set log level to INFO",
        action="store_const",
        dest="log_level",
        const=logging.INFO,
    )

    # archive functionality
    archive = subparsers.add_parser("archive")
    archive.add_argument("target", help="Table to archive", type=str, action="store")
    archive.add_argument(
        "destination",
        help="Dataset to archive to",
        nargs="?",
        type=str,
        default=None,
        action="store",
    )
    archive.add_argument(
        "location",
        help="Location to run job in",
        nargs="?",
        type=str,
        default=None,
        action="store",
    )
    archive.add_argument(
        "overwrite",
        help="Flag indicating whether exising table should be overwritten, default false",
        nargs="?",
        type=bool,
        default=False,
        action="store",
    )

    # parse arguments
    args = parser.parse_args()

    # set logging
    logging.basicConfig(level=args.log_level)

    if args.command == "archive":
        logging.debug("Parsing archive command")
        target = parse_bigquery_reference(ref_type="table", ref=args.target)
        try:
            destination = parse_bigquery_reference(
                ref_type="dataset", ref=args.destination
            )
        except TypeError:
            logging.debug("No valid destination found, using default instead")
            destination = args.destination
        location = args.location
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
                print(f"Archived {res.target} in {res.destination} successfully")
            else:
                print(
                    f"Error encounterd while archiving: {[step for step in res.steps if step.code != 0]}"
                )
        except Exception as err:
            print(f"Unexpected error: {err}")
