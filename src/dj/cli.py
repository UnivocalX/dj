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
    main_parser.add_argument(
        "--s3prefix", type=str, description="S3 prefix for data storage"
    )
    main_parser.add_argument(
        "--s3bucket", type=str, description="S3 bucket for data storage"
    )
    main_parser.add_argument("--s3endpoint", type=str, help="S3 endpoint URL")
    main_parser.add_argument(
        "--registry-endpoint", type=str, help="Registry (db) endpoint URL"
    )
    main_parser.add_argument("--echo", action="store_true", help="Echo SQL commands")
    main_parser.add_argument(
        "--pool-size", type=int, description="Database connection pool size"
    )
    main_parser.add_argument(
        "--max-overflow", type=int, description="Max overflow for database connections"
    )
    main_parser.add_argument("--log-dir", type=str, help="Directory for log files")
    main_parser.add_argument(
        "--verbose",
        action="store_const",
        const=True,
        description="Enable verbose logging",
    )
    main_parser.add_argument(
        "--plain",
        action="store_const",
        const=True,
        description="Disable loading bar and colors",
    )

    # Subparsers
    sub_parsers = main_parser.add_subparsers(dest="command", required=True)

    # Config
    config_parser: ArgumentParser = sub_parsers.add_parser(
        "config", help="configure dj settings."
    )
    config_parser.add_argument(
        "--set-s3endpoint", type=str, description="Set S3 endpoint URL"
    )
    config_parser.add_argument("--set-s3bucket", type=str, description="Set S3 bucket")
    config_parser.add_argument("--set-s3prefix", type=str, description="Set S3 prefix")
    config_parser.add_argument(
        "--set-registry-endpoint",
        type=str,
        description="Set registry (db) endpoint URL",
    )
    config_parser.add_argument(
        "--set-echo", action="store_true", help="Enable SQL command echoing"
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
        "data_src", type=str, description="Source of data files (local or S3)"
    )
    load_parser.add_argument(
        "dataset_name", type=str, description="Name of the dataset"
    )
    load_parser.add_argument("--domain", type=str, description="Domain of the dataset")
    load_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in DataStage],
        description="Data stage",
    )
    load_parser.add_argument(
        "--filters", nargs="+", description="Filters for data files"
    )
    load_parser.add_argument(
        "--exists-ok",
        action="store_const",
        const=True,
        description="Allow loading into existing datasets",
    )
    load_parser.add_argument(
        "--description", type=str, description="Description of the dataset"
    )
    load_parser.add_argument("--tags", nargs="+", description="Tags for the dataset")

    # Fetch
    fetch_parser: ArgumentParser = sub_parsers.add_parser(
        "fetch", help="fetch data from dj registry."
    )
    fetch_parser.add_argument(
        "directory", type=str, description="Directory to save fetched files"
    )
    fetch_parser.add_argument(
        "limit", type=int, description="Limit the number of files to fetch"
    )
    fetch_parser.add_argument("--domain", type=str, description="Domain to filter by")
    fetch_parser.add_argument(
        "--dataset-name", type=str, description="Dataset name to filter by"
    )
    fetch_parser.add_argument(
        "--stage",
        choices=[stage.value for stage in DataStage],
        description="Data stage to filter by",
    )
    fetch_parser.add_argument("--mime", type=str, description="MIME type to filter by")
    fetch_parser.add_argument("--tags", nargs="+", description="Tags to filter by")
    fetch_parser.add_argument(
        "--sha256", nargs="+", description="SHA256 hashes to filter by"
    )
    fetch_parser.add_argument(
        "--filenames", nargs="+", description="File names to filter by"
    )
    fetch_parser.add_argument(
        "--export-format",
        choices=EXPORT_FORMATS,
        description="Format for exporting fetched data",
    )
    fetch_parser.add_argument(
        "--dry",
        action="store_const",
        const=True,
        description="Dry run, do not actually download files",
    )
    fetch_parser.add_argument(
        "--export",
        action="store_const",
        const=True,
        description="Export fetched data to a file",
    )

    return {k: v for k, v in vars(main_parser.parse_args()).items() if v is not None}
