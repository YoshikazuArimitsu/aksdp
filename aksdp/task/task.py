from ..dataset.dataset import DataSet
from abc import ABCMeta, abstractmethod


class Task(metaclass=ABCMeta):
    @abstractmethod
    def main(self, d: DataSet) -> DataSet:
        pass
