from logging import Logger, getLogger

from dj.registry.actions.actor import RegistryActor
from dj.registry.models import FileRecord
from dj.schemes import DeleteDatasetConfig

logger: Logger = getLogger(__name__)


class DataDeleter(RegistryActor):
    def delete_dataset(self, delete_cfg: DeleteDatasetConfig) -> None:
        formatted_dataset_name: str = f"{delete_cfg.domain}/{delete_cfg.name}"

        logger.info(f"Attempting to delete: {formatted_dataset_name}")
        with self.journalist.transaction():
            # Get file records to be deleted
            dataset_file_records: list[FileRecord] = (
                self.journalist.get_file_records_by_dataset(
                    delete_cfg.domain, delete_cfg.name
                )
            )

            # delete records
            self.journalist.delete_dataset(
                domain=delete_cfg.domain,
                name=delete_cfg.name,
            )

            logger.debug(f'decrementing "{len(dataset_file_records)}" ref counts.')
            for file_record in dataset_file_records:
                self._decrement_ref_count(str(file_record.s3uri))

        logger.info(f'Successfully deleted "{formatted_dataset_name}"')