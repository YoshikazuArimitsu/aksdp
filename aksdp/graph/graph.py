from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger
from aksdp.task import Task
from aksdp.dataset import DataSet
from typing import List
from enum import Enum

logger = getLogger(__name__)


class TaskStatus(Enum):
    INIT = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3


class GraphTask:
    def __init__(self, task: Task, dependencies: List["GraphTask"]):
        self.task = task
        self.dependencies = dependencies
        self.status = TaskStatus.INIT
        self.output_ds = None

    def is_runnable(self) -> bool:
        if self.status != TaskStatus.INIT:
            return False

        if self.dependencies is None:
            return True

        return all([gt.status == TaskStatus.COMPLETED for gt in self.dependencies])

    def run(self, ds: DataSet) -> DataSet:
        try:
            logger.debug(f"task({self.task.__class__.__name__}) started.")
            logger.debug(f"  input_ds = {str(ds)}")
            self.status = TaskStatus.RUNNING
            output_ds = self.task.gmain(ds)

            logger.debug(f"task({self.task.__class__.__name__}) completed.")
            logger.debug(f"  output_ds = {str(output_ds)}")
            self.status = TaskStatus.COMPLETED
            self.output_ds = output_ds
        except BaseException as e:
            logger.error(f"task({self.task.__class__.__name__}) failed. {str(e)}")
            self.status = TaskStatus.ERROR
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
            return graph_task.run(input_ds)
        except BaseException as e:
            for eh in self.error_handlers:
                if isinstance(e, eh[0]):
                    self.abort = True
                    eh[1](e, input_ds)
                    return None
            raise

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
