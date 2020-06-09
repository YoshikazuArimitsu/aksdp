from data.json_data import JsonData
from repository.localfile_repository import LocalFileRepository
import unittest
from pathlib import Path
import os
import tempfile


class TestLocalFileRepository(unittest.TestCase):
    def test_read_and_write(self):
        # write
        test_data = {
            "str": "str",
            "number": 42,
            "array": ["a", "r", "r", "a", "y"],
            "object": {"key": "key", "value": "value"},
        }

        tmp_path = Path(tempfile.gettempdir()) / Path(next(tempfile._get_candidate_names()))
        repo = LocalFileRepository(tmp_path)
        data = JsonData(repo, test_data)
        data.save()

        self.assertTrue(tmp_path.exists())
        self.assertTrue(tmp_path.is_file())

        # read
        rdata = JsonData.load(repo)
        self.assertTrue(isinstance(rdata.content, dict))
        self.assertEqual("str", rdata.content["str"])
        self.assertEqual(42, rdata.content["number"])
        self.assertEqual("a", rdata.content["array"][0])
        self.assertEqual("key", rdata.content["object"]["key"])
