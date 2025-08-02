import os
import posixpath
import re
from glob import glob
from importlib.resources import files as resource_files
from logging import Logger, getLogger
from typing import Iterable, TypeVar

from tqdm import tqdm

from dj.constants import ASSETS_DIRECTORY, FALSE_STRINGS, PROGRAM_NAME, TRUE_STRINGS

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
    file_name: str,
    regex: str = r"[^a-zA-Z0-9]",
    case: str = "lower",
) -> str:
    base_name, ext = os.path.splitext(file_name)
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
    directory: str, filters: Iterable[str] | None = None, recursive: bool = False
) -> set[str]:
    filepaths: set[str] = set()
    directory = os.path.abspath(directory)

    logger.debug(f'Collecting files from: "{directory}"')
    if filters:
        filters = set(filters)
        logger.debug(f"Extensions: {', '.join(filters)}")
        for ext in filters:
            pattern = f"**/*.{ext}" if recursive else f"*.{ext}"
            matches = glob(pattern, root_dir=directory, recursive=recursive)
            for match in matches:
                full_path = os.path.join(directory, match)
                if os.path.isfile(full_path):
                    filepaths.add(full_path)
    else:
        pattern = "**/*" if recursive else "*"
        matches = glob(pattern, root_dir=directory, recursive=recursive)
        for match in matches:
            full_path = os.path.join(directory, match)
            if os.path.isfile(full_path):
                filepaths.add(full_path)

    logger.debug(f"Collected {len(filepaths)} file\\s")
    return filepaths


def format_file_size(size_bytes: int, unit: str | None = None) -> str:
    units: list[str] = ["B", "KB", "MB", "GB", "TB"]
    unit: str | None = unit.upper() if unit else None

    if unit and unit in units:
        index = units.index(unit)
        size = size_bytes / (1024**index)
        return f"{size:.2f}{unit}"

    # Auto-scale
    for u in units:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f}{u}"
        size_bytes /= 1024.0

    return f"{size_bytes:.2f}PB"


def pretty_bar(
    iterable: Iterable[T],
    disable: bool = False,
    desc: str = "Processing",
) -> Iterable[T]:
    print()
    return tqdm(
        iterable,
        desc=f"{desc}",
        unit="file",
        ncols=100,
        colour="green",
        disable=disable,
        mininterval=0.05,
        miniters=10,
        bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} {unit} [{elapsed}<{remaining}, {rate_fmt}]",
        leave=True,
    )
