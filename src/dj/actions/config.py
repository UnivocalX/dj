import os
from functools import cached_property
from logging import Logger, getLogger

import yaml

from dj.constants import DJCFG_FILENAME, PROGRAM_NAME
from dj.schemes import ConfigureDJConfig, DJConfig
from dj.utils import pretty_format, resolve_internal_dir

logger: Logger = getLogger(__name__)


class DJManager:
    def __init__(self, cfg: DJConfig | None = None, warn: bool = True):
        self._cfg: DJConfig | None = cfg
        self.warn: bool = warn

    @cached_property
    def cfg_filepath(self) -> str:
        return os.path.join(resolve_internal_dir(), DJCFG_FILENAME)

    @cached_property
    def cfg(self) -> DJConfig:
        # Load config from file if exists
        dict_cfg: dict = {}
        logger.info(f'Loading configuration from "{self.cfg_filepath}"\n')
        if os.path.isfile(self.cfg_filepath):
            with open(self.cfg_filepath, "r") as file:
                dict_cfg = yaml.safe_load(file) or {}
        elif self.warn:
            logger.warning("Missing config file.")

        try:
            # Update with file config if available
            cfg: DJConfig = DJConfig(**dict_cfg)
        except ValueError as e:
            logger.warning(f"Invalid config ({self.cfg_filepath})\n{str(e)}")

        # Override with instance config if provided
        if self._cfg is not None:
            cfg = self._cfg.model_copy(update=cfg.model_dump(exclude_unset=True))
        return cfg

    def configure(self, cfg: ConfigureDJConfig) -> None:
        logger.debug(f"new config: {cfg.model_dump()}")
        current_cfg_dict: dict = self.cfg.model_dump()
        updates: dict = cfg.model_dump(exclude_unset=True)

        # Determine if we actually need to update anything
        needs_update: bool = False
        updated_cfg: dict = current_cfg_dict.copy()

        if "set_s3prefix" in updates and updates["set_s3prefix"] != self.cfg.s3prefix:
            updated_cfg["s3prefix"] = updates["set_s3prefix"]
            needs_update = True

        if "set_s3bucket" in updates and updates["set_s3bucket"] != self.cfg.s3bucket:
            updated_cfg["s3bucket"] = updates["set_s3bucket"]
            needs_update = True

        if (
            "set_s3endpoint" in updates
            and updates["set_s3endpoint"] != self.cfg.s3endpoint
        ):
            updated_cfg["s3endpoint"] = updates["set_s3endpoint"]
            needs_update = True

        if (
            "set_registry_endpoint" in updates
            and updates["set_registry_endpoint"] != self.cfg.registry_endpoint
        ):
            updated_cfg["registry_endpoint"] = updates["set_registry_endpoint"]
            needs_update = True

        if "set_echo" in updates and updates["set_echo"] != self.cfg.echo:
            updated_cfg["echo"] = updates["set_echo"]
            needs_update = True

        if (
            "set_pool_size" in updates
            and updates["set_pool_size"] != self.cfg.pool_size
        ):
            updated_cfg["pool_size"] = updates["set_pool_size"]
            needs_update = True

        if (
            "set_max_overflow" in updates
            and updates["set_max_overflow"] != self.cfg.max_overflow
        ):
            updated_cfg["max_overflow"] = updates["set_max_overflow"]
            needs_update = True

        if needs_update:
            with open(self.cfg_filepath, "w") as file:
                yaml.dump(updated_cfg, file)
            logger.info("Configuration successfully updated")
        else:
            logger.debug("No configuration changes needed")

        logger.info(
            pretty_format(updated_cfg, title=f"\n{PROGRAM_NAME.upper()} Configuration")
        )
