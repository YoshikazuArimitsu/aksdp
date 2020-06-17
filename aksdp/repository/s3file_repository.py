import boto3
import tempfile
from logging import getLogger
from .localfile_repository import LocalFileRepository
from pathlib import Path
from aksdp.data import Data
from urllib.parse import urlparse

logger = getLogger(__name__)


class S3FileRepository(LocalFileRepository):
    """S3上のファイルへのアクセス
    LocalFileの機能に読み込み・保存の前後にS3への転送を挟むだけ。
    """

    def __init__(self, access_key_id: str, secret_access_key: str, url: str):
        o = urlparse(url, allow_fragments=False)

        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / Path(o.path).name
        self.s3_url = url
        self.s3_bucket = o.netloc
        self.s3_key = o.path.lstrip("/")

        logger.debug(
            f"setup S3FileRepository... tempdir={self.tempdir.name}, path={str(self.path)}, s3_bucket={self.s3_bucket}"
            f" s3_key={self.s3_key}"
        )

        self.s3client = boto3.Session(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,).client(
            "s3"
        )

        self._upload_extra_args = {}

    def load(self, ctor):
        logger.debug(f"s3 download {self.s3_url} -> {self.path.resolve()}")
        with open(self.path.resolve(), "wb") as f:
            self.s3client.download_fileobj(self.s3_bucket, self.s3_key, f)
        return super().load(ctor)

    def save(self, data: Data):
        super().save(data)

        logger.debug(f"s3 upload {str(self.path.resolve())} -> {self.s3_url}")
        self.s3client.upload_file(
            str(self.path.resolve()), self.s3_bucket, self.s3_key, ExtraArgs=self.upload_extra_args
        )

    @property
    def upload_extra_args(self) -> dict:
        return self._upload_extra_args

    @upload_extra_args.setter
    def upload_extra_args(self, extra_args: dict):
        self._upload_extra_args = extra_args
