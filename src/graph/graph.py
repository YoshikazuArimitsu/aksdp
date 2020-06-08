from logging import getLogger
from task.task import Task
from dataset.dataset import DataSet
from typing import List
from enum import Enum

logger = getLogger(__name__)


class TaskStatus(Enum):
    INIT = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3


class GraphTask:
    def __init__(self, task: Task, dependencies: List['GraphTask']):
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

    def run(self, ds):
        try:
            logger.debug(f"running task({self.task.__class__.__name__}) ...")
            self.status = TaskStatus.RUNNING
            self.output_ds = self.task.main(ds)

            logger.debug(
                f"task({self.task.__class__.__name__}) completed.")
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            logger.error(
                f"task({self.task.__class__.__name__}) failed. {str(e)}")
            self.status = TaskStatus.ERROR
        return self.output_ds


class Graph:
    def __init__(self):
        self.graph = []

    def append(self, task: Task, dependencies: List[GraphTask] = None):
        gt = GraphTask(task, dependencies)
        self.graph.append(gt)
        return gt

    def runnable_tasks(self) -> List[GraphTask]:
        return [g for g in self.graph if g.is_runnable()]

    def run(self, ds):
        while self.runnable_tasks():
            t = self.runnable_tasks()[0]

            if t.dependencies:
                input_ds = DataSet()
                for d in reversed(t.dependencies):
                    input_ds.merge(d.output_ds)
            else:
                input_ds = ds

            ds = t.run(ds)
        return ds

    def is_all_completed(self) -> bool:
        return all([gt.status == TaskStatus.COMPLETED for gt in self.graph])
