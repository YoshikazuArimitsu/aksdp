from logging import getLogger
from data.data import Data

logger = getLogger(__name__)


class DataSet:
    def __init__(self):
        self.data = {}

    def put(self, name: str, d: Data):
        self.data[name] = d

    def get(self, name: str) -> Data:
        return self.data[name]

    def save_all(self):
        for k, v in self.data.items():
            logger.debug(f"save dataset[{k}]...")
            v.save()

    def apply(self, task):
        return task.main(self)
