from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.dependencies = dependencies
        self.status = TaskStatus.INIT
        self.input_ds = None
        self.output_ds = None
        self._pre_run_hook = self.empty_hook
        self._post_run_hook = self.empty_hook

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
        if self.status != TaskStatus.INIT:
            return False

        if self.dependencies is None:
            return True

        return all([gt.status == TaskStatus.COMPLETED for gt in self.dependencies])

    def run(self, ds: DataSet) -> DataSet:
        try:
            self.input_ds = ds
            logger.debug(f"task({self.task.__class__.__name__}) started.")
            logger.debug(f"  input_ds = {str(ds)}")
            self.pre_run_hook(ds)
            output_ds = self.task.gmain(ds)
            self.post_run_hook(ds)

            logger.debug(f"task({self.task.__class__.__name__}) completed.")
            logger.debug(f"  output_ds = {str(output_ds)}")
            self.output_ds = output_ds
        except BaseException as e:
            logger.error(f"task({self.task.__class__.__name__}) failed. {str(e)}")
            raise
        return self.output_ds


class Graph:
    def __init__(self):
        self.graph = []
        self.error_handlers = []
        self.abort = False

    def append(self, task: Task, dependencies: List[GraphTask] = None) -> GraphTask:
        gt = GraphTask(task, dependencies)
        self.graph.append(gt)
        return gt

    def add_error_handler(self, cls, fn):
        self.error_handlers.append((cls, fn))

    def runnable_tasks(self) -> List[GraphTask]:
        return [g for g in self.graph if g.is_runnable()]

    def _run(self, graph_task, input_ds):
        try:
            graph_task.status = TaskStatus.RUNNING
            r = graph_task.run(input_ds)
            graph_task.status = TaskStatus.COMPLETED
            return r
        except BaseException as e:
            if not self._handle_error(graph_task, input_ds, e):
                raise

    def _handle_error(self, graph_task: GraphTask, input_ds: DataSet, e: BaseException):
        graph_task.status = TaskStatus.ERROR
        for eh in self.error_handlers:
            if isinstance(e, eh[0]):
                self.abort = True
                eh[1](e, input_ds)
                return True

        return False

    def _make_task_inputs(self, graph_task):
        if not graph_task.dependencies:
            return None

        ds = DataSet()
        for d in reversed(graph_task.dependencies):
            ds.merge(d.output_ds)
        return ds

    def run(self, ds: DataSet = None) -> DataSet:
        last_ds = ds
        while self.runnable_tasks() and self.abort is False:
            t = self.runnable_tasks()[0]

            input_ds = self._make_task_inputs(t)
            input_ds = ds if input_ds is None else input_ds

            last_ds = self._run(t, input_ds)
        return last_ds

    def is_all_completed(self) -> bool:
        return all([gt.status == TaskStatus.COMPLETED for gt in self.graph])
