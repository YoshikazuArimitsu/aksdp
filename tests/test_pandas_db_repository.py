from sqlalchemy import create_engine
from sqlalchemy.orm.exc import FlushError
from data.raw_data import RawData
from data.dataframe_data import DataFrameData
from repository.localfile_repository import LocalFileRepository
from repository.pnadas_db_repository import PandasDbRepository
import unittest
from pathlib import Path
import os
import pandas as pd
from logging import basicConfig, DEBUG


class TestPandasDbRepository(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///ut.sqlite3', echo=False)

    def test_dataframe(self):
        repo = LocalFileRepository(
            Path(os.path.dirname(__file__)) / Path('titanic.csv'))
        data = DataFrameData.load(repo)

        db_repo = PandasDbRepository(self.engine, 'titanic')

        dfd = DataFrameData(db_repo, data.content)
        dfd.save()

        dfd2 = DataFrameData.load(db_repo)
        self.assertIsNotNone(dfd2.content)
        self.assertTrue(data.content.equals(dfd2.content))
