from logging import getLogger
from .repository import Repository
from pathlib import Path
from data.data import Data, DataType

logger = getLogger(__name__)


class LocalFileRepository(Repository):
    def __init__(self, path: Path):
        super().__init__()
        self.path = path

    def save(self, data: Data):
        if data.data_type == DataType.RAW:
            self.path.write_bytes(data.content)
        elif data.data_type == DataType.DATAFRAME:
            data.content.to_csv(str(self.path), index=False)
        else:
            raise ValueError(
                'LocalFileRepository.save() not support DataType {data.data_type}')

    def load(self, ctor):
        raw_data = self.path.read_bytes()
        return ctor(self, raw_data)
