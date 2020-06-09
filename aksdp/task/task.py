from aksdp.dataset import DataSet
from abc import ABCMeta, abstractmethod


class Task(metaclass=ABCMeta):
    @abstractmethod
    def main(self, d: DataSet) -> DataSet:
        pass

    def gmain(self, d: DataSet) -> DataSet:
        return self.main(d)
