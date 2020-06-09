from .data import Data, DataType
import pandas as pd
from logging import getLogger
from typing import TypeVar

logger = getLogger(__name__)
TRepository = TypeVar("Repository")


class SqlAlchemyModelData(Data):
    def __init__(self, repository: TRepository, model_class):
        super().__init__(repository, DataType.SQLALCHEMY_MODEL)
        self.model_class = model_class
        self.content_ = []

    def to_dataframe(self) -> pd.DataFrame:
        """モデルのリストを DataFrame に変換する"""
        columns = [c.name for c in self.model_class.__table__.columns]

        df = pd.DataFrame(columns=columns)
        for r in self.content:
            series_data = [getattr(r, c.name) for c in self.model_class.__table__.columns]
            series = pd.Series(series_data, index=df.columns)
            df = df.append(series, ignore_index=True)

        return df

    def update_dataframe(self, df: pd.DataFrame):
        """DataFrameの内容をモデルのリストに書き戻す"""
        append_entities = []

        def update_model(inst, row):
            for c in self.model_class.__table__.columns:
                setattr(inst, c.name, row[c.name])

        for index, row in df.iterrows():
            if index >= len(self.content):
                inst = self.model_class()
                append_entities.append(inst)
            else:
                inst = self.content[index]

            update_model(inst, row)

        # DataFrameから削除された行にマッチするモデルの削除
        indexes = list(df.index.values)
        for idx in reversed(range(0, len(self.content))):
            if idx not in indexes:
                m = self.content.pop(idx)
                self.repository.delete(m)

        # 増えた分をINSERT
        append_models = list(map(lambda x: self.repository.insert(x), append_entities))
        self.content.extend(append_models)

    def query(self, qf=None):
        q = self.repository.query(self.model_class)
        if qf:
            q = qf(q)
        self.content_ = list(q.all())

    @property
    def content(self) -> list:
        return self.content_

    def __str__(self) -> str:
        return f"SqlAlchemyModelData: {len(self.content)} records"
