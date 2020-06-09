from sqlalchemy import create_engine
from aksdp.data import DataFrameData
from aksdp.repository import LocalFileRepository, PandasDbRepository
import unittest
from pathlib import Path
import os


class TestPandasDbRepository(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///ut.sqlite3", echo=False)

    def test_dataframe(self):
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("titanic.csv"))
        data = DataFrameData.load(repo)

        db_repo = PandasDbRepository(self.engine, "titanic")

        dfd = DataFrameData(db_repo, data.content)
        dfd.save()

        dfd2 = DataFrameData.load(db_repo)
        self.assertIsNotNone(dfd2.content)
        self.assertTrue(data.content.equals(dfd2.content))
