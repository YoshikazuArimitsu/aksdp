from logging import getLogger
from .repository import Repository
from pathlib import Path
from aksdp.data import Data, DataType
import json

logger = getLogger(__name__)


class LocalFileRepository(Repository):
    def __init__(self, path: Path):
        super().__init__()
        self.path = path

    def save(self, data: Data):
        if data.data_type == DataType.RAW:
            self.path.write_bytes(data.content)
        elif data.data_type == DataType.JSON:
            json_str = json.dumps(data.content, ensure_ascii=False)
            self.path.write_bytes(json_str.encode("utf-8"))
        elif data.data_type == DataType.DATAFRAME:
            data.content.to_csv(str(self.path), index=False)
        else:
            raise ValueError("LocalFileRepository.save() not support DataType {data.data_type}")

    def load(self, ctor):
        raw_data = self.path.read_bytes()
        return ctor(raw_data, self)
