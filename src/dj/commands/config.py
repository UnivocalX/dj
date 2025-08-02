import os
import warnings
from functools import cached_property
from logging import Logger, getLogger

import yaml

from dj.constants import DJCFG_FILENAME
from dj.schemes import DJCFG, ConfigureDJCFG
from dj.utils import resolve_internal_dir

logger: Logger = getLogger(__name__)


class DJManager:
    def __init__(self, cfg: DJCFG | None = None, warn: bool = True):
        self._cfg: DJCFG | None = cfg
        self.warn: bool = warn

    @cached_property
    def cfg_filepath(self) -> str:
        return os.path.join(resolve_internal_dir(), DJCFG_FILENAME)

    @cached_property
    def cfg(self) -> DJCFG:
        not self.warn or warnings.filterwarnings("default")

        # Load config from file if exists
        dict_cfg: dict = {}
        if os.path.isfile(self.cfg_filepath):
            with open(self.cfg_filepath, "r") as file:
                dict_cfg = yaml.safe_load(file) or {}
        else:
            warnings.warn(f"Missing config file ({self.cfg_filepath}).")

        # Start with default config
        cfg: DJCFG = DJCFG()

        try:
            # Update with file config if available
            if dict_cfg:
                cfg = DJCFG(**dict_cfg)
        except ValueError as e:
            warnings.warn(f"Invalid config file ({self.cfg_filepath})\n{str(e)}")

        # Override with instance config if provided
        if self._cfg is not None:
            cfg = self._cfg.model_copy(update=cfg.model_dump(exclude_unset=True))

        warnings.filterwarnings("ignore")
        return cfg

    def configure(self, cfg: ConfigureDJCFG) -> None:
        logger.debug(f"new config: {cfg.model_dump()}")
        current_cfg_dict: dict[str] = self.cfg.model_dump()
        updates: dict[str] = cfg.model_dump(exclude_unset=True)

        # Determine if we actually need to update anything
        needs_update: bool = False
        updated_cfg: dict[str] = current_cfg_dict.copy()

        if "set_s3prefix" in updates and updates["set_s3prefix"] != self.cfg.s3prefix:
            updated_cfg["s3prefix"] = updates["set_s3prefix"]
            needs_update = True

        if "set_s3bucket" in updates and updates["set_s3bucket"] != self.cfg.s3bucket:
            updated_cfg["s3bucket"] = updates["set_s3bucket"]
            needs_update = True

        if "set_verbose" in updates and updates["set_verbose"] != self.cfg.verbose:
            updated_cfg["verbose"] = updates["set_verbose"]
            needs_update = True

        if "set_log_dir" in updates and updates["set_log_dir"] != self.cfg.log_dir:
            updated_cfg["log_dir"] = updates["set_log_dir"]
            needs_update = True

        if "enable_colors" in updates and updates["enable_colors"] != self.cfg.colors:
            updated_cfg["colors"] = updates["enable_colors"]
            needs_update = True

        if needs_update:
            with open(self.cfg_filepath, "w") as file:
                yaml.dump(updated_cfg, file)
            logger.info(f"Configuration successfully updated ({self.cfg_filepath})")
        else:
            logger.debug("No configuration changes needed")
