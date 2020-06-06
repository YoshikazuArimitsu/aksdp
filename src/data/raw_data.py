from logging import getLogger
from repository.repository import Repository
from .data import Data, DataType

logger = getLogger(__name__)


class RawData(Data):
    def __init__(self, repository: Repository, content: bytes = None):
        super().__init__(repository, DataType.RAW)
        self.content_ = content

    @classmethod
    def load(cls, repository):
        return repository.load(RawData.create)

    @classmethod
    def create(cls, repository, raw_data):
        return RawData(repository, raw_data)

    @property
    def content(self):
        return self.content_
