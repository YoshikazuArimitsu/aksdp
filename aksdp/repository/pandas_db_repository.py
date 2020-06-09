from logging import getLogger
from ..data.data import Data, DataType
from io import StringIO
import pandas as pd
from .repository import Repository

logger = getLogger(__name__)


class PandasDbRepository(Repository):
    def __init__(self, engine, table_name):
        super().__init__()
        self.engine = engine
        self.table_name = table_name

    def save(self, data: Data):
        if data.data_type == DataType.DATAFRAME:
            data.content.to_sql(self.table_name, self.engine, if_exists="replace", index=False)
        else:
            raise ValueError("PandasDbRepository.save() not support DataType {data.data_type}")

    def load(self, ctor):
        sio = StringIO()
        df = pd.read_sql_table(self.table_name, self.engine)
        df.to_csv(sio, index=False)
        return ctor(self, sio.getvalue().encode("utf-8"))
