from logging import getLogger, basicConfig, DEBUG
import os
from graph.graph import Graph
from pathlib import Path
from task.task import Task
from dataset.dataset import DataSet
from data.dataframe_data import DataFrameData
from repository.localfile_repository import LocalFileRepository

logger = getLogger(__name__)


class FillNaMedian(Task):
    """指定列の欠損値を中央値で埋めるTask
    """

    def __init__(self, column):
        self.column = column

    def main(self, ds):
        df = ds.get('titanic').content
        df[self.column] = df[self.column].fillna(df[self.column].median())
        return ds


class SexToCode(Task):
    """性別をcodeに変換するTask
    """

    def main(self, ds):
        df = ds.get('titanic').content
        df["Sex"][df["Sex"] == "male"] = 0
        df["Sex"][df["Sex"] == "female"] = 1
        return ds


class EmbarkedToCode(Task):
    def main(self, ds):
        df = ds.get('titanic').content
        df["Embarked"] = df["Embarked"].fillna("S")
        df["Embarked"][df["Embarked"] == "S"] = 0
        df["Embarked"][df["Embarked"] == "C"] = 1
        df["Embarked"][df["Embarked"] == "Q"] = 2
        return ds


if __name__ == '__main__':
    basicConfig(level=DEBUG)

    # データセットの読み込み
    ds = DataSet()
    repo = LocalFileRepository(
        Path(os.path.dirname(__file__)) / Path('../titanic.csv'))
    titanic_data = DataFrameData.load(repo)
    ds.put('titanic', titanic_data)

    #
    print("## Original data")
    print(ds.get('titanic').content)

    # Graphで処理する
    # Age欠損埋め -> 性別のコード化 -> 乗船した港 のコード化 の順で処理
    graph = Graph()
    fill_age = graph.append(FillNaMedian("Age"))
    sex_to_code = graph.append(SexToCode(), [fill_age])
    graph.append(EmbarkedToCode(), [sex_to_code])
    ds = graph.run(ds)

    print("## Processed data")
    print(ds.get('titanic').content)
