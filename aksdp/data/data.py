from logging import getLogger
from enum import Enum

logger = getLogger(__name__)


class DataType(Enum):
    RAW = 0
    DATAFRAME = 1
    JSON = 2
    SQLALCHEMY_MODEL = 3


class Data:
    def __init__(self, repository, data_type: DataType = DataType.RAW):
        self.repository = repository
        self.data_type_ = data_type

    def save(self):
        if self.repository:
            self.repository.save(self)

    @property
    def data_type(self) -> DataType:
        return self.data_type_
