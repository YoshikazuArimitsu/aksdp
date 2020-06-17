from logging import getLogger
from aksdp.data import Data
from typing import List

logger = getLogger(__name__)


class DataSet:
    def __init__(self):
        self.data = {}

    def put(self, name: str, d: Data) -> "DataSet":
        self.data[name] = d
        return self

    def get(self, name: str) -> Data:
        return self.data[name]

    def keys(self) -> List[str]:
        return self.data.keys()

    def merge(self, ds: "DataSet") -> "DataSet":
        if ds:
            for k, v in ds.data.items():
                self.data[k] = v
        return self

    def save_all(self):
        for k, v in self.data.items():
            logger.debug(f"save dataset[{k}]...")
            v.save()

    def apply(self, task) -> "DataSet":
        return task.main(self)

    def __str__(self) -> str:
        keys = ",".join(self.keys())
        return f"DataSet({keys})"
