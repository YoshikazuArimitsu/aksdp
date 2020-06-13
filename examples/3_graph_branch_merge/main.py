import argparse
import os
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from logging import DEBUG, basicConfig, getLogger
from pathlib import Path

from aksdp.data import DataFrameData
from aksdp.dataset import DataSet
from aksdp.graph import ConcurrentGraph, Graph
from aksdp.repository import LocalFileRepository
from aksdp.task import Task
from aksdp.util import PlantUML

logger = getLogger(__name__)


class PassThrough(Task):
    def output_datakeys(self):
        return ["titanic"]

    def main(self, ds):
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

        time.sleep(random.randint(3, 10))
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

        time.sleep(random.randint(3, 10))
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

        time.sleep(random.randint(3, 10))
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
        rds.put("titanic", DataFrameData(df))
        return rds


if __name__ == "__main__":
    basicConfig(level=DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--graph", type=str, default="normal", help="Graph {normal/thread/process}")
    args = parser.parse_args()

    if args.graph == "normal":
        graph = Graph()
        logger.info("use Graph")
    elif args.graph == "thread":
        graph = ConcurrentGraph(ThreadPoolExecutor())
        logger.info("use ConcurrentGraph(ThreadPoolExecutor)")
    elif args.graph == "process":
        graph = ConcurrentGraph(ProcessPoolExecutor())
        logger.info("use ConcurrentGraph(ProcessPoolExecutor)")
    else:
        logger.error("unknown graph,")
        sys.exit(-1)

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

    passthrough = graph.append(PassThrough())
    fill_age = graph.append(FillNaMedian("Age"))
    sex_to_code = graph.append(SexToCode())
    embarked_to_code = graph.append(EmbarkedToCode())

    graph.append(Merge(), [passthrough, fill_age, sex_to_code, embarked_to_code])

    ds = graph.run(ds)

    print("## Processed data")
    print(ds.get("titanic").content)

    print(f"PlantUML Diagraph : {PlantUML.graph_to_url(graph)}")
