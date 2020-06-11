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
from aksdp.graph import Graph
from aksdp.repository import LocalFileRepository
from aksdp.task import Task
from aksdp.util import AirFlow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

logger = getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 11),
    "email": ["yoshikazu_arimitsu@albert2005.co.jp"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("graph_to_dag", default_args=default_args, schedule_interval=timedelta(1))


class ReadCsv(Task):
    def output_datakeys(self):
        return ["titanic"]

    def main(self, ds):
        ds = DataSet()
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("titanic.csv"))
        titanic_data = DataFrameData.load(repo)
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
        rds.put(f"fillna_{self.column}", DataFrameData(None, df[self.column]))

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
        df["Sex"][df["Sex"] == "male"] = 0
        df["Sex"][df["Sex"] == "female"] = 1

        rds = DataSet()
        rds.put("sex_to_code", DataFrameData(None, df["Sex"]))

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
        df["Embarked"][df["Embarked"] == "S"] = 0
        df["Embarked"][df["Embarked"] == "C"] = 1
        df["Embarked"][df["Embarked"] == "Q"] = 2

        rds = DataSet()
        rds.put("embarked_to_code", DataFrameData(None, df["Embarked"]))

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
        rds.put("titanic", DataFrameData(None, df))
        return rds


graph = Graph()
readcsv = graph.append(ReadCsv())
fill_age = graph.append(FillNaMedian("Age"), [readcsv])
sex_to_code = graph.append(SexToCode(), [readcsv])
embarked_to_code = graph.append(EmbarkedToCode(), [readcsv])

graph.append(Merge(), [readcsv, fill_age, sex_to_code, embarked_to_code])


af = AirFlow(graph)
af.to_dag(graph)
