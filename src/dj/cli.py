from argparse import ArgumentParser
from importlib.metadata import version
from logging import Logger, getLogger

from dj.constants import EXPORT_FORMATS, DataStage

logger: Logger = getLogger(__name__)


def parser(prog_name: str) -> dict:
    main_parser: ArgumentParser = ArgumentParser(prog=prog_name)

    # Global flags
    main_parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version(prog_name)}"
    )
    main_parser.add_argument("--s3prefix", type=str, help="S3 prefix for data storage")
    main_parser.add_argument("--s3bucket", type=str, help="S3 bucket for data storage")
    main_parser.add_argument("--s3endpoint", type=str, help="S3 endpoint URL")
    main_parser.add_argument(
        "--registry-endpoint", type=str, help="Registry (db) endpoint URL"
    )
    main_parser.add_argument(
        "--echo", action="store_const", const=True, help="Echo SQL commands"
    )
    main_parser.add_argument(
        "--pool-size", type=int, help="Database connection pool size"
    )
    main_parser.add_argument(
        "--max-overflow", type=int, help="Max overflow for database connections"
    )
    main_parser.add_argument("--log-dir", type=str, help="Directory for log files")
    main_parser.add_argument(
        "--verbose",
        action="store_const",
        const=True,
        help="Enable verbose logging",
    )
    main_parser.add_argument(
        "--plain",
        action="store_const",
        const=True,
        help="Disable loading bar and colors",
    )

    # Subparsers
    sub_parsers = main_parser.add_subparsers(dest="command", required=True)

    # Config
    config_parser: ArgumentParser = sub_parsers.add_parser(
        "config", help="configure dj settings."
    )
    config_parser.add_argument("--set-s3endpoint", type=str, help="Set S3 endpoint URL")
    config_parser.add_argument("--set-s3bucket", type=str, help="Set S3 bucket")
    config_parser.add_argument("--set-s3prefix", type=str, help="Set S3 prefix")
    config_parser.add_argument(
        "--set-registry-endpoint",
        type=str,
        help="Set registry (db) endpoint URL",
    )
    config_parser.add_argument(
        "--set-echo",
        action="store_const",
        const=True,
        help="Enable SQL command echoing",
    )
    config_parser.add_argument(
        "--set-pool-size", type=int, help="Set database connection pool size"
    )
    config_parser.add_argument(
        "--set-max-overflow", type=int, help="Set max overflow for database connections"
    )

    # Load
    load_parser: ArgumentParser = sub_parsers.add_parser(
        "load", help="load data into dj registry."
    )
    load_parser.add_argument(
        "data_src",
        type=str,
        help="Source of data files (local or S3), local support glob patterns, S3 support filters.",
    )
    load_parser.add_argument("dataset_name", type=str, help="Name of the dataset")
    load_parser.add_argument("--domain", type=str, help="Domain of the dataset")
    load_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in DataStage],
        help="Data stage",
    )
    load_parser.add_argument(
        "--filters",
        nargs="+",
        help="Filter files by extension. will overwrite glob patterns.",
    )
    load_parser.add_argument(
        "--exists-ok",
        action="store_const",
        const=True,
        help="Allow loading into existing datasets",
    )
    load_parser.add_argument(
        "--description", type=str, help="Description of the dataset"
    )
    load_parser.add_argument("--tags", nargs="+", help="Tags for the dataset")

    # Fetch
    fetch_parser: ArgumentParser = sub_parsers.add_parser(
        "fetch", help="fetch data from dj registry."
    )
    fetch_parser.add_argument(
        "directory", type=str, help="Directory to save fetched files"
    )
    fetch_parser.add_argument(
        "limit", type=int, help="Limit the number of files to fetch"
    )
    fetch_parser.add_argument("--domain", type=str, help="Domain to filter by")
    fetch_parser.add_argument(
        "--dataset-name", type=str, help="Dataset name to filter by"
    )
    fetch_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in DataStage],
        help="Data stage to filter by",
    )
    fetch_parser.add_argument("--mime", type=str, help="MIME type to filter by")
    fetch_parser.add_argument("--tags", nargs="+", help="Tags to filter by")
    fetch_parser.add_argument("--sha256", nargs="+", help="SHA256 hashes to filter by")
    fetch_parser.add_argument("--filenames", nargs="+", help="File names to filter by")
    fetch_parser.add_argument(
        "--export-format",
        choices=EXPORT_FORMATS,
        help="Format for exporting fetched data",
    )
    fetch_parser.add_argument(
        "--dry",
        action="store_const",
        const=True,
        help="Dry run, do not actually download files",
    )
    fetch_parser.add_argument(
        "--export",
        action="store_const",
        const=True,
        help="Export fetched data to a file",
    )
    fetch_parser.add_argument(
        "--overwrite",
        action="store_const",
        const=True,
        help="Overwrite existing files during fetch",
    )
    fetch_parser.add_argument(
        "--flat",
        action="store_const",
        const=True,
        help="Store files in a flat structure without subdirectories",
    )

    # List
    list_parser: ArgumentParser = sub_parsers.add_parser(
        "list", help="list datasets in the registry."
    )
    list_parser.add_argument("--domain", type=str, help="Domain to filter datasets by")
    list_parser.add_argument(
        "--name-pattern", type=str, help="Pattern to filter dataset names"
    )
    list_parser.add_argument(
        "--limit", type=int, help="Limit the number of datasets to list"
    )
    list_parser.add_argument(
        "--offset", type=int, help="Offset for pagination of datasets"
    )

    return {k: v for k, v in vars(main_parser.parse_args()).items() if v is not None}
