from logging import getLogger
from .data import Data, DataType
import pandas as pd
from io import StringIO
from typing import TypeVar

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class DataFrameData(Data):
    def __init__(self, content: pd.DataFrame, repository: TRepository = None):
        super().__init__(repository, DataType.DATAFRAME)
        self.content_ = content

    @classmethod
    def load(cls, repository: TRepository) -> "DataFrameData":
        return repository.load(DataFrameData.create_from_csv)

    @classmethod
    def create_from_csv(cls, raw_data: bytes, repository: TRepository = None) -> "DataFrameData":
        sio = StringIO(raw_data.decode("utf-8"))
        df = pd.read_csv(sio)
        return DataFrameData(df, repository)

    @classmethod
    def create_from_df(cls, df: pd.DataFrame, repository: TRepository = None) -> "DataFrameData":
        return DataFrameData(df, repository)

    @property
    def content(self) -> pd.DataFrame:
        return self.content_

    def __str__(self) -> str:
        return f"DataFrameData:¥n{self.content.head()}"
