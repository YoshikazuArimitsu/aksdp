from logging import getLogger
from .data import Data, DataType
import pandas as pd
from io import StringIO
from typing import TypeVar

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class DataFrameData(Data):
    def __init__(self, repository: TRepository, content: pd.DataFrame = None):
        super().__init__(repository, DataType.DATAFRAME)
        self.content_ = content

    @classmethod
    def load(cls, repository: TRepository) -> "DataFrameData":
        return repository.load(DataFrameData.create_from_csv)

    @classmethod
    def create_from_csv(cls, repository: TRepository, raw_data) -> "DataFrameData":
        sio = StringIO(raw_data.decode("utf-8"))
        df = pd.read_csv(sio)
        return DataFrameData(repository, df)

    @classmethod
    def create_from_df(cls, repository: TRepository, df) -> "DataFrameData":
        return DataFrameData(repository, df)

    @property
    def content(self) -> pd.DataFrame:
        return self.content_

    def __str__(self) -> str:
        return f"DataFrameData:¥n{self.content.head()}"
