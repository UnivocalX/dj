#!python3.12
from logging import Logger, getLogger

from dj.cli import parser
from dj.commands.config import DJManager
from dj.commands.fetch import DataFetcher
from dj.commands.load import DataLoader
from dj.constants import PROGRAM_NAME
from dj.logging import configure_logging
from dj.schemes import (
    ConfigureDJConfig,
    DJConfig,
    DJConfigCLI,
    FetchDataConfig,
    LoadDataConfig,
)

logger: Logger = getLogger(PROGRAM_NAME)


def main() -> None:
    parsed_args: dict = parser(PROGRAM_NAME)
    dj_cli_cfg: DJConfigCLI = DJConfigCLI(**parsed_args)
    configure_logging(
        PROGRAM_NAME,
        log_dir=dj_cli_cfg.log_dir,
        plain=dj_cli_cfg.plain,
        verbose=dj_cli_cfg.verbose,
    )

    dj_manager: DJManager = DJManager(DJConfig(**parsed_args))

    logger.debug(f"CLI Arguments: {parsed_args}")
    logger.debug(f"DJ CLI Config: {dj_cli_cfg.model_dump()}")
    logger.debug(f"DJ Config: {dj_manager.cfg.model_dump()}")

    match dj_cli_cfg.command:
        case "config":
            dj_manager.configure(ConfigureDJConfig(**parsed_args))
        case "load":
            dj_cfg: DJConfig = DJConfig(**dj_manager.cfg.model_dump(), **parsed_args)
            load_cfg: LoadDataConfig = LoadDataConfig(**parsed_args)

            with DataLoader(dj_cfg) as data_loader:
                data_loader.load(load_cfg)

        case "fetch":
            dj_cfg: DJConfig = DJConfig(**dj_manager.cfg.model_dump(), **parsed_args)
            fetch_cfg: FetchDataConfig = FetchDataConfig(**parsed_args)

            with DataFetcher(dj_cfg) as data_fetcher:
                data_fetcher.fetch(fetch_cfg)
