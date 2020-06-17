from aksdp.dataset import DataSet
from abc import ABCMeta, abstractmethod
from typing import List
import time


class Task(metaclass=ABCMeta):
    def __init__(self):
        """.ctor
        """
        self._in = {}
        self._elapsed_time = None

    @abstractmethod
    def main(self, d: DataSet) -> DataSet:
        """タスクの処理

        Args:
            d (DataSet): 入力DataSet

        Returns:
            DataSet: 出力DataSet
        """
        pass

    def gmain(self, d: DataSet) -> DataSet:
        """タスクの処理(Graphから呼び出す用)

        Args:
            d (DataSet): 入力DataSet

        Returns:
            DataSet: 出力DataSet
        """
        self._in = {}

        # DataSet 自動展開
        if d:
            for k in d.keys():
                self._in[k] = d.get(k).content

        # 実行
        start = time.time()
        r = self.main(d)
        self._elapsed_time = time.time() - start

        return r

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

    @property
    def elapsed_time(self):
        """実行時間の取得

        Returns:
            timedelta : 実行時間
        """
        return self._elapsed_time
