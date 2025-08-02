#!python3.12
from logging import Logger, getLogger

import yaml

from dj.cli import parser
from dj.commands.config import DJManager
from dj.commands.load import DataLoader
from dj.constants import PROGRAM_NAME
from dj.logging import configure_logging
from dj.schemes import DJCFG, ConfigureDJCFG, LoadDataCFG

logger: Logger = getLogger(PROGRAM_NAME)


def main() -> None:
    cli_cfg: dict[str] = parser(PROGRAM_NAME)
    dj_manager: DJManager = DJManager(DJCFG(**cli_cfg))

    log_filepath: str = configure_logging(
        PROGRAM_NAME,
        log_dir=dj_manager.cfg.log_dir,
        enable_colors=dj_manager.cfg.colors,
        verbose=dj_manager.cfg.verbose,
    )

    logger.debug(f"DJ Config: {dj_manager.cfg.model_dump()}")
    logger.debug(f"CLI Arguments: {cli_cfg}")
    logger.debug(f"Log file path: {log_filepath}")

    match cli_cfg["command"]:
        case "config":
            dj_manager.configure(ConfigureDJCFG(**cli_cfg))

            if cli_cfg.get("show"):
                cfg_content: str = yaml.dump(dj_manager.cfg.model_dump())
                logger.info(
                    f"{PROGRAM_NAME.upper()} Configuration:\n"
                    "----------------------\n"
                    f"{cfg_content}"
                )
                logger.info(f"Config file: {dj_manager.cfg_filepath}")
        case "load":
            data_loader: DataLoader = DataLoader(dj_manager.cfg, LoadDataCFG(**cli_cfg))
            data_loader.load()
