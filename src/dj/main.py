#!python3.12
from logging import Logger, getLogger
from sys import exit as sys_exit

from dj.actions.config import DJManager
from dj.actions.registry.catalog import DataCatalog
from dj.actions.registry.create import DatasetCreator
from dj.actions.registry.journalist import Journalist
from dj.actions.registry.load import DataLoader
from dj.actions.registry.tags import DataTagger
from dj.cli import parser
from dj.constants import PROGRAM_NAME
from dj.exceptions import DatasetExist, FailedToGatherFiles
from dj.logging import configure_logging
from dj.schemes import (
    ConfigureDJConfig,
    CreateDatasetConfig,
    Dataset,
    DJConfig,
    DJConfigCLI,
    ExportDataConfig,
    FetchDataConfig,
    ListDatasetsConfig,
    LoadDataConfig,
    TagsConfig,
)
from dj.utils import pretty_format

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

    dj_cfg: DJConfig = dj_manager.cfg.model_copy(update=parsed_args)
    match dj_cli_cfg.command:
        case "config":
            dj_manager.configure(ConfigureDJConfig(**parsed_args))

        case "create":
            try:
                with DatasetCreator(dj_cfg) as dataset_creator:
                    dataset_creator.create(CreateDatasetConfig(**parsed_args))
            except DatasetExist as e:
                logger.error(e)
                sys_exit(1)

        case "list":
            list_cfg = ListDatasetsConfig(**parsed_args)

            with Journalist(dj_cfg) as journalist:
                datasets: list[Dataset] = journalist.list_datasets(
                    domain=list_cfg.domain,
                    name_pattern=list_cfg.name_pattern,
                    limit=list_cfg.limit,
                    offset=list_cfg.offset,
                )

            for dataset in datasets:
                logger.info(pretty_format(dataset.model_dump(), title=dataset.name))

        case "load":
            try:
                with DataLoader(dj_cfg) as data_loader:
                    data_loader.load(LoadDataConfig(**parsed_args))
            except (DatasetExist, FailedToGatherFiles) as e:
                logger.error(e)
                sys_exit(1)

        case "fetch":
            with DataCatalog(dj_cfg) as data_catalog:
                data_catalog.fetch(FetchDataConfig(**parsed_args))

        case "export":
            with DataCatalog(dj_cfg) as data_catalog:
                data_catalog.export(ExportDataConfig(**parsed_args))

        case "tags":
            match dj_cli_cfg.subcommand:
                case "add":
                    with DataTagger(dj_cfg) as data_tagger:
                        data_tagger.add(TagsConfig(**parsed_args))
                case "remove":
                    with DataTagger(dj_cfg) as data_tagger:
                        data_tagger.remove(TagsConfig(**parsed_args))
