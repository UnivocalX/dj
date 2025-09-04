#!python3.12
from logging import Logger, getLogger
from sys import exit as sys_exit

from dj.cli import parser
from dj.constants import PROGRAM_NAME
from dj.exceptions import (
    DatasetExist,
    DatasetNotFound,
    FailedToGatherFiles,
    FileRecordNotFound,
    TagNotFound,
    UnsuffiecentPermissions,
)
from dj.logging import configure_logging
from dj.registry.actions.catalog import DataCatalog
from dj.registry.actions.delete import DataDeleter
from dj.registry.actions.enforce import PolicyEnforcer
from dj.registry.actions.load import DataLoader
from dj.registry.actions.tag import DataTagger
from dj.registry.config import RegistryConfigManager
from dj.registry.journalist import Journalist
from dj.schemes import (
    ConfigureRegistryConfig,
    CreateDatasetConfig,
    Dataset,
    DeleteDatasetConfig,
    DJConfigCLI,
    ExportDataConfig,
    FetchDataConfig,
    ListDatasetsConfig,
    LoadDataConfig,
    RegistryConfig,
    TagConfig,
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

    registry_config_manager: RegistryConfigManager = RegistryConfigManager(
        RegistryConfig(**parsed_args)
    )

    logger.debug(f"CLI Arguments: {parsed_args}")
    logger.debug(f"DJ CLI Config: {dj_cli_cfg.model_dump()}")
    logger.debug(f"Registry Config: {registry_config_manager.cfg.model_dump()}")

    registry_cfg: RegistryConfig = registry_config_manager.cfg.model_copy(
        update=parsed_args
    )
    try:
        match dj_cli_cfg.command:
            case "config":
                registry_config_manager.configure(
                    ConfigureRegistryConfig(**parsed_args)
                )

            case "create":
                with DataLoader(registry_cfg) as dataset_loader:
                    dataset_loader.create_dataset(CreateDatasetConfig(**parsed_args))

            case "list":
                list_cfg = ListDatasetsConfig(**parsed_args)
                with Journalist(registry_cfg) as journalist:
                    datasets: list[Dataset] = journalist.list_datasets(
                        domain=list_cfg.domain,
                        name_pattern=list_cfg.name_pattern,
                        limit=list_cfg.limit,
                        offset=list_cfg.offset,
                    )

                for dataset in datasets:
                    logger.info(pretty_format(dataset.model_dump(), title=dataset.name))

            case "load":
                with DataLoader(registry_cfg) as data_loader:
                    data_loader.load(LoadDataConfig(**parsed_args))

            case "fetch":
                with DataCatalog(registry_cfg) as data_catalog:
                    data_catalog.fetch(FetchDataConfig(**parsed_args))

            case "export":
                with DataCatalog(registry_cfg) as data_catalog:
                    data_catalog.export(ExportDataConfig(**parsed_args))

            case "delete":
                with DataDeleter(registry_cfg) as data_deleter:
                    data_deleter.delete_dataset(DeleteDatasetConfig(**parsed_args))

            case "enforce":
                PolicyEnforcer(registry_cfg).enforce()
            case "tags":
                match dj_cli_cfg.subcommand:
                    case "add":
                        with DataTagger(registry_cfg) as data_tagger:
                            data_tagger.add(TagConfig(**parsed_args))
                    case "remove":
                        with DataTagger(registry_cfg) as data_tagger:
                            data_tagger.remove(TagConfig(**parsed_args))

    except (
        UnsuffiecentPermissions,
        DatasetExist,
        DatasetNotFound,
        FailedToGatherFiles,
        FileRecordNotFound,
        TagNotFound,
    ) as e:
        logger.error(e)
        sys_exit(1)
