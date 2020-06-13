from logging import getLogger
from .data import Data, DataType
from io import StringIO
import json
from typing import TypeVar

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class JsonData(Data):
    def __init__(self, content: dict, repository: TRepository = None):
        super().__init__(repository, DataType.JSON)
        self.content_ = content

    @classmethod
    def load(cls, repository: TRepository) -> "JsonData":
        return repository.load(JsonData.create_from_json)

    @classmethod
    def create_from_json(cls, raw_data: bytes, repository: TRepository) -> "JsonData":
        sio = StringIO(raw_data.decode("utf-8"))
        content = json.load(sio)
        return JsonData(content, repository)

    @property
    def content(self) -> dict:
        return self.content_

    @property
    def __str__(self) -> str:
        return f"JsonData:{self.content}"
