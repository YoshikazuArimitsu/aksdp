from logging import getLogger
from repository.repository import Repository
from .data import Data, DataType
from io import StringIO
import json

logger = getLogger(__name__)


class JsonData(Data):
    def __init__(self, repository: Repository, content: dict):
        super().__init__(repository, DataType.JSON)
        self.content_ = content

    @classmethod
    def load(cls, repository):
        return repository.load(JsonData.create_from_json)

    @classmethod
    def create_from_json(cls, repository, raw_data):
        sio = StringIO(raw_data.decode("utf-8"))
        content = json.load(sio)
        return JsonData(repository, content)

    @property
    def content(self):
        return self.content_
