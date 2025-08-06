from enum import Enum


class DataStage(str, Enum):
    RAW = "raw"
    STAGED = "staged"
    PROCESSED = "processed"
    PUBLISHED = "published"

PROGRAM_NAME: str = "dj"
DJCFG_FILENAME: str = "config.yaml"
ASSETS_DIRECTORY: str = "assets"
FETCH_FILENAME: str = 'fetch'
EXPORT_FORMATS: list[str] = ['yaml', 'yml', 'json']
TRUE_STRINGS: list[str] = ["yes", "true", "t", "y", "1"]
FALSE_STRINGS: list[str] = ["no", "false", "f", "n", "0"]

DEFAULT_DOMAIN: str = "global"
