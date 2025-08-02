from argparse import ArgumentParser
from importlib.metadata import version
from logging import Logger, getLogger

from dj.utils import str2bool

logger: Logger = getLogger(__name__)


def parser(prog_name: str) -> dict[str]:
    main_parser: ArgumentParser = ArgumentParser(prog=prog_name)

    # Global flags
    main_parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version(prog_name)}"
    )
    main_parser.add_argument("--s3prefix", type=str)
    main_parser.add_argument("--s3bucket", type=str)
    main_parser.add_argument("--log-dir", type=str)
    main_parser.add_argument("--verbose", action="store_const", const=True)
    main_parser.add_argument("--colors", action="store_const", const=True)

    # Subparsers
    sub_parsers = main_parser.add_subparsers(dest="command", required=True)

    # Config
    config_parser: ArgumentParser = sub_parsers.add_parser(
        "config", help="configure dj"
    )
    config_parser.add_argument("--show", action="store_true")
    config_parser.add_argument("--set-s3bucket", type=str)
    config_parser.add_argument("--set-s3prefix", type=str)
    config_parser.add_argument("--set-verbose", type=str2bool)
    config_parser.add_argument("--enable-colors", type=str2bool)

    # Load
    load_parser: ArgumentParser = sub_parsers.add_parser(
        "load", help="load data into dj datasets"
    )
    load_parser.add_argument("file_src", type=str)
    load_parser.add_argument("dataset_id", type=str)
    load_parser.add_argument("--stage", type=str)
    load_parser.add_argument("--domain", type=str)
    load_parser.add_argument("--filters", nargs="+")
    load_parser.add_argument("--overwrite", action="store_const", const=True)

    return {k: v for k, v in vars(main_parser.parse_args()).items() if v is not None}
