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
    main_parser.add_argument("--s3prefix", type=str)
    main_parser.add_argument("--s3bucket", type=str)
    main_parser.add_argument("--s3endpoint", type=str)
    main_parser.add_argument("--log-dir", type=str)
    main_parser.add_argument("--verbose", action="store_const", const=True)
    main_parser.add_argument("--plain", action="store_const", const=True)

    # Subparsers
    sub_parsers = main_parser.add_subparsers(dest="command", required=True)

    # Config
    config_parser: ArgumentParser = sub_parsers.add_parser(
        "config", help="configure dj."
    )
    config_parser.add_argument("--set-s3bucket", type=str)
    config_parser.add_argument("--set-s3prefix", type=str)
    config_parser.add_argument("--set-s3endpoint", type=str)

    # Load
    load_parser: ArgumentParser = sub_parsers.add_parser(
        "load", help="load data into dj registry."
    )
    load_parser.add_argument("data_src", type=str)
    load_parser.add_argument("dataset_name", type=str)
    load_parser.add_argument("--domain", type=str)
    load_parser.add_argument("--stage", choices=[stage.value for stage in DataStage])
    load_parser.add_argument("--filters", nargs="+")
    load_parser.add_argument("--exists-ok", action="store_const", const=True)
    load_parser.add_argument("--description", type=str)
    load_parser.add_argument("--tags", nargs="+")
    
    # Fetch
    fetch_parser: ArgumentParser = sub_parsers.add_parser(
        "fetch", help="fetch data from dj registry."
    )
    fetch_parser.add_argument('directory', type=str)
    fetch_parser.add_argument('limit', type=int)
    fetch_parser.add_argument('--domain', type=str)
    fetch_parser.add_argument('--dataset-name', type=str)
    fetch_parser.add_argument('--stage', choices=[stage.value for stage in DataStage])
    fetch_parser.add_argument('--mime', type=str)
    fetch_parser.add_argument('--tags', nargs="+")
    fetch_parser.add_argument('--export-format', choices=EXPORT_FORMATS)
    fetch_parser.add_argument("--dry", action="store_const", const=True)
    fetch_parser.add_argument("--export", action="store_const", const=True)
    
    return {k: v for k, v in vars(main_parser.parse_args()).items() if v is not None}
