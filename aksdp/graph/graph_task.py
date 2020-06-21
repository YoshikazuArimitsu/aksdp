from logging import getLogger
from aksdp.task import Task
from aksdp.dataset import DataSet
from typing import List, Callable
from enum import Enum

logger = getLogger(__name__)


class TaskStatus(Enum):
    INIT = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3


class GraphTask(object):
    def __init__(self, task: Task, dependencies: List["GraphTask"]):
        """.ctor

        Args:
            task (Task): 実行タスク
            dependencies (List): 依存タスク
        """
        self.task = task
        self.status = TaskStatus.INIT
        self.input_ds = None
        self.output_ds = None
        self._pre_run_hook = self.empty_hook
        self._post_run_hook = self.empty_hook

        self._dependencies_static = dependencies if dependencies else []
        self._dependencies_dynamic = []

    @property
    def dependencies(self) -> List["GraphTask"]:
        return self._dependencies_static + self._dependencies_dynamic

    @property
    def dependencies_dynamic(self):
        return self._dependencies_dynamic

    @dependencies_dynamic.setter
    def dependencies_dynamic(self, tasks: List["GraphTask"]):
        self._dependencies_dynamic = tasks

    @property
    def pre_run_hook(self) -> Callable:
        """実行前フックの取得

        Returns:
            Callable: 実行前フック関数
        """
        return self._pre_run_hook

    @pre_run_hook.setter
    def pre_run_hook(self, v: Callable):
        """実行前フックの設定

        Args:
            v (Callable): 実行前フック関数
        """
        self._pre_run_hook = v

    @property
    def post_run_hook(self) -> Callable:
        """実行後フックの取得

        Returns:
            Callable: 実行後フック関数
        """
        return self._post_run_hook

    @post_run_hook.setter
    def post_run_hook(self, v: Callable):
        """実行後フックの設定

        Args:
            v (Callable): 実行後フック関数
        """
        self._post_run_hook = v

    def empty_hook(self, ds: DataSet):
        """デフォルトの空フック

        Args:
            ds (DataSet): データセット
        """
        pass

    def is_runnable(self) -> bool:
        """実行可能状態(未実行であり、かつ依存タスクが全て完了状態かどうか)

        Returns:
            bool: 実行可能状態
        """
        if self.status != TaskStatus.INIT:
            return False

        if self.dependencies is None:
            return True

        return all([gt.status == TaskStatus.COMPLETED for gt in self.dependencies])

    def run(self, ds: DataSet = None) -> "GraphTask":
        """タスク実行

        Args:
            ds (DataSet, optional): 入力DataSet. Defaults to None.

        Returns:
            GraphTask: 実行後のGraphTask
        """
        try:
            self.input_ds = ds
            self.status = TaskStatus.RUNNING
            logger.debug(f"task({self.task.__class__.__name__}) started.")
            logger.debug(f"  input_ds = {str(ds)}")
            self.pre_run_hook(ds)
            output_ds = self.task.gmain(ds)
            self.post_run_hook(output_ds)

            logger.debug(f"task({self.task.__class__.__name__}) completed. (elapse={self.task.elapsed_time:.3f}s)")
            logger.debug(f"  output_ds = {str(output_ds)}")
            self.output_ds = output_ds
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            logger.error(f"task({self.task.__class__.__name__}) failed. {str(e)}")
            self.status = TaskStatus.ERROR
            raise
        return self
