import os
from logging import DEBUG, basicConfig, getLogger
from pathlib import Path

from aksdp.data import DataFrameData
from aksdp.dataset import DataSet
from aksdp.graph import DebugGraph
from aksdp.repository import LocalFileRepository
from aksdp.task import Task

logger = getLogger(__name__)


class LoadCsv(Task):
    """CSVファイルを読み込むタスク
    """

    def output_datakeys(self):
        return ["titanic"]

    def main(self, ds):
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("../titanic.csv"))
        titanic_data = DataFrameData.load(repo)

        ds = DataSet()
        ds.put("titanic", titanic_data)
        return ds


class FillNaMedian(Task):
    """指定列の欠損値を中央値で埋めるTask
    """

    def __init__(self, column):
        self.column = column

    def input_datakeys(self):
        return ["titanic"]

    def output_datakeys(self):
        return [f"fillna_{self.column}"]

    def main(self, ds):
        df = ds.get("titanic").content
        df[self.column] = df[self.column].fillna(df[self.column].median())

        rds = DataSet()
        rds.put(f"fillna_{self.column}", DataFrameData(df[self.column]))
        return rds


class SexToCode(Task):
    """性別をcodeに変換するTask
    """

    def input_datakeys(self):
        return ["titanic"]

    def output_datakeys(self):
        return ["sex_to_code"]

    def main(self, ds):
        df = ds.get("titanic").content
        df.loc[df["Sex"] == "male", "Sex"] = 0
        df.loc[df["Sex"] == "female", "Sex"] = 1

        rds = DataSet()
        rds.put("sex_to_code", DataFrameData(df["Sex"]))
        return rds


class EmbarkedToCode(Task):
    def input_datakeys(self):
        return ["titanic"]

    def output_datakeys(self):
        return ["embarked_to_code"]

    def main(self, ds):
        df = ds.get("titanic").content
        df["Embarked"] = df["Embarked"].fillna("S")
        df.loc[df["Embarked"] == "S", "Embarked"] = 0
        df.loc[df["Embarked"] == "C", "Embarked"] = 1
        df.loc[df["Embarked"] == "Q", "Embarked"] = 2

        rds = DataSet()
        rds.put("embarked_to_code", DataFrameData(df["Embarked"]))
        return rds


class Merge(Task):
    def input_datakeys(self):
        return ["titanic", "fillna_Age", "sex_to_code", "embarked_to_code"]

    def main(self, ds):
        df = ds.get("titanic").content

        df = df.drop(["Age", "Sex", "Embarked"], axis=1)
        df = df.join(ds.get("fillna_Age").content)
        df = df.join(ds.get("sex_to_code").content)
        df = df.join(ds.get("embarked_to_code").content)

        rds = DataSet()
        rds.put("titanic_result", DataFrameData(df))
        return rds


if __name__ == "__main__":
    basicConfig(level=DEBUG)

    graph = DebugGraph(Path("./dump"))
    load_csv = graph.append(LoadCsv())
    fill_age = graph.append(FillNaMedian("Age"))
    sex_to_code = graph.append(SexToCode())
    embarked_to_code = graph.append(EmbarkedToCode())
    merge = graph.append(Merge())

    # ds = graph.run()
    graph.run_task(merge)
