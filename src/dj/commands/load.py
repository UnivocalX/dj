import os
import tempfile
from contextlib import contextmanager
from logging import Logger, getLogger

from dj.inspect import FileInspector
from dj.schemes import FileMetadata, LoadDataConfig
from dj.storage import Storage
from dj.utils import collect_files, merge_s3uri, pretty_bar, pretty_format

logger: Logger = getLogger(__name__)


def resolve_data_s3uri(
    s3bucket: str,
    s3prefix: str,
    domain: str,
    dataset_id: str,
    stage: str,
    mime_type: str,
    filename: str,
) -> str:
    return merge_s3uri(
        s3bucket, s3prefix, domain, dataset_id, stage, mime_type, filename
    )


class DataLoader:
    def __init__(self, cfg: LoadDataConfig):
        self.cfg: LoadDataConfig = cfg
        self.storage: Storage = Storage(cfg)

    def _gather_datafiles(self) -> set[str]:
        datafiles: set[str] = set()

        logger.info(f"attempting to load data, filters: {self.cfg.filters}")
        if self.cfg.data_src.startswith("s3://"):
            logger.info("gathering data from S3")

            s3objcets: list[str] = self.storage.list_objects(
                self.cfg.data_src,
                self.cfg.filters,
            )

            for s3obj in s3objcets:
                datafiles.add(merge_s3uri(self.cfg.data_src, s3obj))
        else:
            logger.info("gathering data from local storage")
            datafiles = collect_files(self.cfg.data_src, self.cfg.filters)

        logger.info(f'Gathered {len(datafiles)} file\\s from "{self.cfg.data_src}"')
        return datafiles

    def _process_datafile(self, datafile_src: str) -> tuple[str, FileMetadata]:
        with self._get_local_file(datafile_src) as local_path:
            metadata: FileMetadata = self._inspect_file(local_path)
            dst_s3uri: str = self._upload2s3(local_path, metadata)
            return dst_s3uri, metadata

    @contextmanager
    def _get_local_file(self, datafile_src: str):
        if datafile_src.startswith("s3://"):
            tmpfile: str = os.path.join(
                tempfile.gettempdir(), os.path.basename(datafile_src)
            )
            self.storage.download_obj(datafile_src, tmpfile)
            try:
                yield tmpfile
            finally:
                if os.path.exists(tmpfile):
                    os.remove(tmpfile)
        else:
            yield datafile_src

    def _inspect_file(self, local_path: str) -> FileMetadata:
        inspector: FileInspector = FileInspector(local_path)
        return inspector.metadata

    def _upload2s3(self, local_path: str, metadata: FileMetadata) -> str:
        dst_s3uri: str = resolve_data_s3uri(
            self.cfg.s3bucket,
            self.cfg.s3prefix,
            self.cfg.domain,
            self.cfg.dataset_id,
            self.cfg.stage,
            metadata.mime_type,
            metadata.filename,
        )
        # self.storage.upload(local_path, dst_s3uri)
        return dst_s3uri

    def load(self) -> None:
        datafiles: set[str] = self._gather_datafiles()
        if not datafiles:
            raise ValueError(f"Failed to gather data files from {self.cfg.data_src}")

        logger.info(f"Starting to process {len(datafiles)} file\\s")
        processed_datafiles: dict[str, FileMetadata] = {}
        for datafile in pretty_bar(
            datafiles, disable=self.cfg.plain, desc="⚙️ Processing", unit="file"
        ):
            try:
                dst_s3uri, metadata = self._process_datafile(datafile)
            except Exception as e:
                logger.debug(e)
                logger.error(f"Failed to load {datafile}.")
            else:
                data: dict = metadata.model_dump(exclude={'filepath'})
                data['S3URI'] = dst_s3uri
                logger.info(pretty_format(data, title='Loaded File Metadata'))
                processed_datafiles[dst_s3uri] = metadata

        if not processed_datafiles:
            raise ValueError(f"Failed to load datafiles ({self.cfg.data_src}).")

        logger.info(f"Successfully loaded: {len(processed_datafiles)} file\\s.")
