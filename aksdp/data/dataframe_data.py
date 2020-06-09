from logging import getLogger
from .data import Data, DataType
import pandas as pd
from io import StringIO

logger = getLogger(__name__)


class DataFrameData(Data):
    def __init__(self, repository, content: pd.DataFrame = None):
        super().__init__(repository, DataType.DATAFRAME)
        self.content_ = content

    @classmethod
    def load(cls, repository):
        return repository.load(DataFrameData.create_from_csv)

    @classmethod
    def create_from_csv(cls, repository, raw_data):
        sio = StringIO(raw_data.decode("utf-8"))
        df = pd.read_csv(sio)
        return DataFrameData(repository, df)

    @classmethod
    def create_from_df(cls, repository, df):
        return DataFrameData(repository, df)

    @property
    def content(self):
        return self.content_
