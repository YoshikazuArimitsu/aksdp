from logging import getLogger, basicConfig, DEBUG
from aksdp.repository import LocalFileRepository
from aksdp.data import DataFrameData
from aksdp.dataset import DataSet
from aksdp.task import Task
from pathlib import Path
import os

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

    # 欠損処理・性別/乗船した港のコード変換等いくつかの処理をチェーンするサンプル
    if True:
        ds = FillNaMedian("Age").main(ds)
        ds = SexToCode().main(ds)
        ds = EmbarkedToCode().main(ds)
    else:
        ds = ds.apply(FillNaMedian("Age")).apply(SexToCode()).apply(EmbarkedToCode())

    print("## Processed data")
    print(ds.get("titanic").content)
