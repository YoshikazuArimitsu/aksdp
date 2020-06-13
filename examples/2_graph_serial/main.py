from logging import getLogger, basicConfig, DEBUG
import os
from aksdp.graph import Graph
from pathlib import Path
from aksdp.task import Task
from aksdp.dataset import DataSet
from aksdp.data import DataFrameData
from aksdp.repository import LocalFileRepository

logger = getLogger(__name__)


class FillNaMedian(Task):
    """指定列の欠損値を中央値で埋めるTask
    """

    def __init__(self, column):
        self.column = column

    def main(self, ds):
        df = ds.get("titanic").content
        df[self.column] = df[self.column].fillna(df[self.column].median())

        ds = DataSet()
        ds.put("titanic", DataFrameData(df))
        return ds


class SexToCode(Task):
    """性別をcodeに変換するTask
    """

    def main(self, ds):
        df = ds.get("titanic").content
        df.loc[df["Sex"] == "male", "Sex"] = 0
        df.loc[df["Sex"] == "female", "Sex"] = 1

        ds = DataSet()
        ds.put("titanic", DataFrameData(df))
        return ds


class EmbarkedToCode(Task):
    def main(self, ds):
        df = ds.get("titanic").content
        df["Embarked"] = df["Embarked"].fillna("S")
        df.loc[df["Embarked"] == "S", "Embarked"] = 0
        df.loc[df["Embarked"] == "C", "Embarked"] = 1
        df.loc[df["Embarked"] == "Q", "Embarked"] = 2

        ds = DataSet()
        ds.put("titanic", DataFrameData(df))
        return ds


if __name__ == "__main__":
    basicConfig(level=DEBUG)

    # データセットの読み込み
    ds = DataSet()
    repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("../titanic.csv"))
    titanic_data = DataFrameData.load(repo)
    ds.put("titanic", titanic_data)

    #
    print("## Original data")
    print(ds.get("titanic").content)

    # Graphで処理する
    # Age欠損埋め -> 性別のコード化 -> 乗船した港 のコード化 の順で処理
    graph = Graph()
    fill_age = graph.append(FillNaMedian("Age"))
    sex_to_code = graph.append(SexToCode(), [fill_age])
    graph.append(EmbarkedToCode(), [sex_to_code])
    ds = graph.run(ds)

    print("## Processed data")
    print(ds.get("titanic").content)
