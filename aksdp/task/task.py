from aksdp.dataset import DataSet
from abc import ABCMeta, abstractmethod
from typing import List


class Task(metaclass=ABCMeta):
    @abstractmethod
    def main(self, d: DataSet) -> DataSet:
        pass

    def gmain(self, d: DataSet) -> DataSet:
        return self.main(d)

    def input_datakeys(self) -> List[str]:
        """入力DataSetの中から実際に使用するデータの key を列挙する

        Returns:
            List[str]: 入力で使用するデータの key
        """
        return []

    def output_datakeys(self) -> List[str]:
        """出力DataSetに使用するデータのkeyを列挙する

        Returns:
            List[str]: 出力に使用するデータのkey
        """
        return []
