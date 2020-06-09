from aksdp.data import RawData, DataFrameData
from aksdp.repository import S3FileRepository, LocalFileRepository
import unittest
from pathlib import Path
import os


class TestS3FileRepository(unittest.TestCase):
    def setUp(self):
        # TODO:このテストを実行するには AWSのアクセスキー・テスト用バケットが必要です
        self.access_key_id = os.getenv("aws_access_key_id")
        self.secret_access_key = os.getenv("aws_secret_access_key")
        self.s3file_url = os.getenv("s3file_url")

    def test_raw(self):
        # ローカルのファイルを読んでS3に保存
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("titanic.csv"))

        data = RawData.load(repo)
        self.assertIsNotNone(data)

        repo_s3 = S3FileRepository(self.access_key_id, self.secret_access_key, self.s3file_url)
        data.repository = repo_s3
        data.save()

    def test_dataframe(self):
        # ローカルのファイルを読んでS3に保存
        repo = LocalFileRepository(Path(os.path.dirname(__file__)) / Path("titanic.csv"))

        data = DataFrameData.load(repo)

        repo_s3 = S3FileRepository(self.access_key_id, self.secret_access_key, self.s3file_url)

        data.repository = repo_s3
        data.save()

        # S3からファイルを読み込み
        data2 = DataFrameData.load(repo_s3)
        self.assertTrue(len(data2.content) > 0)
