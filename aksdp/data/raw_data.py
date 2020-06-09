from logging import getLogger
from .data import Data, DataType
from typing import TypeVar

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class RawData(Data):
    def __init__(self, repository: TRepository, content: bytes = None):
        super().__init__(repository, DataType.RAW)
        self.content_ = content

    @classmethod
    def load(cls, repository: TRepository) -> "RawData":
        return repository.load(RawData.create)

    @classmethod
    def create(cls, repository, raw_data) -> "RawData":
        return RawData(repository, raw_data)

    @property
    def content(self) -> bytes:
        return self.content_

    def __str__(self) -> str:
        return f"RawData: {len(self.content)}bytes, b'{self.content[:16]}...'"
