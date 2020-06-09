from logging import getLogger, basicConfig, DEBUG
import os
from pathlib import Path
from aksdp.graph import Graph, ConcurrentGraph
from aksdp.task import Task
from aksdp.dataset import DataSet
from aksdp.data import DataFrameData
from aksdp.repository import LocalFileRepository

logger = getLogger(__name__)


class PassThrough(Task):
    def main(self, ds):
        return ds


class FillNaMedian(Task):
    """指定列の欠損値を中央値で埋めるTask
    """

    def __init__(self, column):
        self.column = column

    def main(self, ds):
        df = ds.get("titanic").content
        df[self.column] = df[self.column].fillna(df[self.column].median())

        rds = DataSet()
        rds.put(f"fillna_{self.column}", DataFrameData(None, df[self.column]))
        return rds


class SexToCode(Task):
    """性別をcodeに変換するTask
    """

    def main(self, ds):
        df = ds.get("titanic").content
        df["Sex"][df["Sex"] == "male"] = 0
        df["Sex"][df["Sex"] == "female"] = 1

        rds = DataSet()
        rds.put("sex_to_code", DataFrameData(None, df["Sex"]))
        return rds


class EmbarkedToCode(Task):
    def main(self, ds):
        df = ds.get("titanic").content
        df["Embarked"] = df["Embarked"].fillna("S")
        df["Embarked"][df["Embarked"] == "S"] = 0
        df["Embarked"][df["Embarked"] == "C"] = 1
        df["Embarked"][df["Embarked"] == "Q"] = 2

        rds = DataSet()
        rds.put("embarked_to_code", DataFrameData(None, df["Embarked"]))
        return rds


class Merge(Task):
    def main(self, ds):
        df = ds.get("titanic").content

        df = df.drop(["Age", "Sex", "Embarked"], axis=1)
        df = df.join(ds.get("fillna_Age").content)
        df = df.join(ds.get("sex_to_code").content)
        df = df.join(ds.get("embarked_to_code").content)

        rds = DataSet()
        rds.put("titanic", DataFrameData(None, df))
        return rds


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
    # Age欠損埋め・性別のコード化・乗船した港のコード化 を順不同で実行してマージ
    graph = Graph()

    passthrough = graph.append(PassThrough())
    fill_age = graph.append(FillNaMedian("Age"))
    sex_to_code = graph.append(SexToCode())
    embarked_to_code = graph.append(EmbarkedToCode())

    graph.append(Merge(), [passthrough, fill_age, sex_to_code, embarked_to_code])

    ds = graph.run(ds)

    print("## Processed data")
    print(ds.get("titanic").content)
