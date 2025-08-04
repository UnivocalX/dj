#!python3.12
from logging import Logger, getLogger

from dj.cli import parser
from dj.commands.config import DJManager
from dj.commands.load import DataLoader
from dj.constants import PROGRAM_NAME
from dj.logging import configure_logging
from dj.schemes import ConfigureDJConfig, DJConfig, DJConfigCLI, LoadDataConfig

logger: Logger = getLogger(PROGRAM_NAME)


def main() -> None:
    parsed_arguments: dict[str] = parser(PROGRAM_NAME)
    dj_cli_cfg: DJConfigCLI = DJConfigCLI(**parsed_arguments)
    configure_logging(
        PROGRAM_NAME,
        log_dir=dj_cli_cfg.log_dir,
        plain=dj_cli_cfg.plain,
        verbose=dj_cli_cfg.verbose,
    )

    dj_manager: DJManager = DJManager(DJConfig(**parsed_arguments))

    logger.debug(f"CLI Arguments: {parsed_arguments}")
    logger.debug(f"DJ CLI Config: {dj_cli_cfg.model_dump()}")
    logger.debug(f"DJ Config: {dj_manager.cfg.model_dump()}")

    match dj_cli_cfg.command:
        case "config":
            dj_manager.configure(ConfigureDJConfig(**dj_manager))
        case "load":
            data_loader: DataLoader = DataLoader(
                LoadDataConfig(**dj_manager.cfg.model_dump(), **parsed_arguments)
            )
            data_loader.load()
