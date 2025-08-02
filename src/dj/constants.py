from enum import Enum


class DataStage(str, Enum):
    RAW = "raw"
    STAGED = "staged"
    PROCESSED = "processed"
    PUBLISHED = "published"


PROGRAM_NAME: str = "dj"
DJCFG_FILENAME: str = "config.yaml"
ASSETS_DIRECTORY: str = "assets"

TRUE_STRINGS: list[str] = ["yes", "true", "t", "y", "1"]
FALSE_STRINGS: list[str] = ["no", "false", "f", "n", "0"]
