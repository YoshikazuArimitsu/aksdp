from data.raw_data import RawData
from data.dataframe_data import DataFrameData
from repository.localfile_repository import LocalFileRepository
import unittest
from pathlib import Path
import os
import tempfile


class TestLocalFileRepository(unittest.TestCase):
    def test_raw(self):
        repo = LocalFileRepository(
            Path(os.path.dirname(__file__)) / Path('titanic.csv'))

        data = RawData.load(repo)
        self.assertIsNotNone(data)

        tmp_path = Path(tempfile.gettempdir()) / \
            Path(next(tempfile._get_candidate_names()))
        repo = LocalFileRepository(tmp_path)
        data.repository = repo
        data.save()

        self.assertTrue(tmp_path.exists())
        self.assertTrue(tmp_path.is_file())

    def test_dataframe(self):
        repo = LocalFileRepository(
            Path(os.path.dirname(__file__)) / Path('titanic.csv'))

        data = DataFrameData.load(repo)

        tmp_path = Path(tempfile.gettempdir()) / \
            Path(next(tempfile._get_candidate_names()))
        repo = LocalFileRepository(tmp_path)
        data.repository = repo
        data.save()

        self.assertTrue(tmp_path.exists())
        self.assertTrue(tmp_path.is_file())
