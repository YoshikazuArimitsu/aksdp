from abc import ABCMeta, abstractmethod
from enum import Enum
from logging import getLogger
from typing import Generic, TypeVar, Any

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class DataType(Enum):
    """DataType定義
    """

    RAW = 0
    DATAFRAME = 1
    JSON = 2
    SQLALCHEMY_MODEL = 3


class Data(metaclass=ABCMeta):
    def __init__(self, repository: TRepository, data_type: DataType = DataType.RAW):
        """.ctor

        Args:
            repository (TRepository): リポジトリ
            data_type (DataType, optional): データの種類. Defaults to DataType.RAW.
        """
        self.repository = repository
        self.data_type_ = data_type

    def save(self):
        """保存
        """
        if self.repository:
            self.repository.save(self)

    @property
    def data_type(self) -> DataType:
        """DataType取得

        Returns:
            DataType: DataType
        """
        return self.data_type_

    @property
    def content(self) -> Any:
        """データの中身へのアクセス

        Returns:
            Any: データの中身
        """
        return None
