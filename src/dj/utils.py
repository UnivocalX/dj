import json
import os
import posixpath
import re
from glob import glob
from importlib.resources import files as resource_files
from logging import Logger, getLogger
from time import sleep
from typing import Any, Iterable, TypeVar
from urllib.parse import quote

import yaml
from pydantic.alias_generators import to_pascal as snake2pascal
from tqdm import tqdm

from dj.constants import (
    ASSETS_DIRECTORY,
    DEFAULT_DELAY,
    FALSE_STRINGS,
    PROGRAM_NAME,
    TRUE_STRINGS,
)

logger: Logger = getLogger(__name__)

T = TypeVar("T")


def str2bool(v) -> bool | None:
    if isinstance(v, bool):
        return v
    if v.lower() in TRUE_STRINGS:
        return True
    elif v.lower() in FALSE_STRINGS:
        return False
    else:
        raise ValueError(f'Cant convert "{v}" to a bool')


def hours2seconds(hours: float) -> int:
    return int(hours * 3600)


def seconds2hours(seconds: int) -> float:
    return round(seconds / 3600, 4)


def resolve_internal_dir() -> str:
    return os.path.expanduser(os.path.join("~/", "." + PROGRAM_NAME))


def serialize_string(
    input_str: str,
    regex_pattern: str = r"[^a-z0-9]",
    replacement: str = "",
    force_lowercase: bool = True,
) -> str:
    if force_lowercase:
        input_str = input_str.lower()

    pattern = re.compile(regex_pattern)
    cleaned: str = pattern.sub(replacement, input_str)

    return cleaned


def split_s3uri(s3uri: str) -> tuple[str, str]:
    if not s3uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3uri}. Must start with 's3://'")

    # Remove the s3:// prefix
    path: str = s3uri[5:]

    if not path:
        raise ValueError("Invalid S3 URI: No bucket specified")

    # Split on first '/' to separate bucket from prefix
    parts: list[str] = path.split("/", 1)
    s3bucket: str = parts[0]
    s3prefix: str = parts[1] if len(parts) > 1 else ""

    if not s3bucket:
        raise ValueError("Invalid S3 URI: Empty bucket name")

    return s3bucket, s3prefix


def merge_s3uri(*parts: str) -> str:
    if not parts:
        raise ValueError("Bucket name cannot be empty")

    return f"s3://{posixpath.join(*parts)}"


def load_asset(file_name: str) -> str:
    asset_file = resource_files(ASSETS_DIRECTORY).joinpath(file_name)
    logger.debug(f"loading asset file: {asset_file}")
    return asset_file.read_text()


def get_directory_size(directory: str) -> float:
    total_bytes: int = 0

    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            filepath: str = os.path.join(dirpath, filename)
            total_bytes += os.path.getsize(filepath)

    dir_size_gp: float = total_bytes / (1024**3)
    logger.debug(f'directory size: "{dir_size_gp}"')
    return dir_size_gp


def clean_string(
    filename: str,
    regex: str = r"[^a-zA-Z0-9/.]",
    case: str = "lower",
) -> str:
    base_name, ext = os.path.splitext(filename)
    cleaned_base = re.sub(regex, "", base_name)

    if case == "lower":
        cleaned_base = cleaned_base.lower()
        ext = ext.lower()
    elif case == "upper":
        cleaned_base = cleaned_base.upper()
        ext = ext.upper()

    cleaned_name = cleaned_base + ext

    return cleaned_name


def collect_files(
    pattern: str, filters: Iterable[str] | None = None, recursive: bool = False
) -> set[str]:
    filepaths: set[str] = set()
    abs_pattern: str = os.path.abspath(pattern)

    # If pattern is a file, just return it
    if os.path.isfile(abs_pattern):
        filepaths.add(abs_pattern)
        return filepaths

    # If pattern is a directory, we need to add wildcards for glob to work
    if os.path.isdir(abs_pattern):
        if recursive:
            pattern = os.path.join(pattern, "**", "*")
        else:
            pattern = os.path.join(pattern, "*")

    logger.debug(f'Collecting files, pattern: "{pattern}"')
    matches = glob(pattern, recursive=recursive)
    for match in matches:
        full_path = os.path.abspath(match)
        if os.path.isfile(full_path):
            filepaths.add(full_path)

    if filters:
        filters = set(filters)
        logger.debug(f"Extensions: {', '.join(filters)}")
        filepaths = {f for f in filepaths if any(f.endswith(ext) for ext in filters)}

    logger.debug(f"Collected {len(filepaths)} file(s)")
    return filepaths


def format_file_size(size_bytes: int, unit: str | None = None) -> str:
    units: list[str] = ["B", "KB", "MB", "GB", "TB", "PB"]
    original_size: float = float(size_bytes)
    result: str = ""

    if unit:
        unit = unit.upper()
        if unit in units:
            index: int = units.index(unit)
            size: float = original_size / (1024**index)
            result = f"{size:.2f}{unit}"
        else:
            result = f"{original_size:.2f}B"
    else:
        size = original_size
        for u in units:
            if size < 1024.0 or u == units[-1]:
                result = f"{size:.2f}{u}"
                break
            size /= 1024.0

    return result


def pretty_bar(
    iterable: Iterable[T],
    disable: bool = False,
    unit: str = "it",
    desc: str = "Processing",
) -> Iterable[T]:
    print()
    try:
        total = len(list(iterable))
    except TypeError:
        total = None

    # Set miniters to 1% of total, but at least 1
    miniters = max(1, int((total or 100) * 0.01))

    return tqdm(
        iterable,
        desc=f"{desc}",
        ncols=100,
        unit=unit,
        colour="green",
        disable=disable,
        mininterval=0.05,
        miniters=miniters,
        bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} {unit} [{elapsed}<{remaining}, {rate_fmt}]",
        leave=True,
    )


def pretty_format(
    data: dict,
    sep: str = ": ",
    indent: int = 4,
    title: str | None = None,
) -> str:
    """
    Format a dict as:
    <Title>:
        - Key: Value
    Keys are converted to CamelCase.
    """
    lines = []
    if title:
        lines.append(f"{title}:")
    prefix = " " * indent + "- "
    for k, v in data.items():
        camel_key = snake2pascal(str(k))
        lines.append(f"{prefix}{camel_key}{sep}{v}")
    return "\n".join(lines) + "\n"


def resolve_data_s3uri(
    s3bucket: str,
    s3prefix: str,
    stage: str,
    mime_type: str,
    sha256: str,
    ext: str | None = None,
) -> str:
    def clean(part: str) -> str:
        return quote(str(part).strip("/ ")) if part else ""

    path: str = "/".join(
        clean(part) for part in [s3prefix, stage, mime_type, sha256] if part
    )
    s3uri_no_ext: str = f"s3://{clean(s3bucket)}/{path}"
    s3uri: str = s3uri_no_ext + ext if ext else s3uri_no_ext
    return s3uri


def export_data(filepath: str, data: dict[str, Any]) -> None:
    format: str = os.path.splitext(filepath)[1].lower()
    abs_filepath: str = os.path.abspath(filepath)

    os.makedirs(os.path.dirname(abs_filepath), exist_ok=True)
    logger.debug(f"exporting data -> {filepath}")
    with open(filepath, "w") as export_file:
        if format == ".json":
            json.dump(data, export_file, indent=4)
        elif format == ".yaml" or format == ".yml":
            yaml.dump(data, export_file, default_flow_style=False, indent=4)
        else:
            raise ValueError(
                f"Unsupported file format: {format}. Supported formats: .json, .yaml, .yml, .csv"
            )


def delay(seconds: int | None = None) -> None:
    logger.debug(f"Delaying for {seconds} seconds...")
    sleep(seconds or DEFAULT_DELAY)
