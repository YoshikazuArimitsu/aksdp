from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger
from ..task.task import Task
from ..dataset.dataset import DataSet
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

    def run(self, ds):
        try:
            logger.debug(f"running task({self.task.__class__.__name__}) ...")
            self.status = TaskStatus.RUNNING
            self.output_ds = self.task.main(ds)

            logger.debug(f"task({self.task.__class__.__name__}) completed.")
            self.status = TaskStatus.COMPLETED
        except BaseException as e:
            logger.error(f"task({self.task.__class__.__name__}) failed. {str(e)}")
            self.status = TaskStatus.ERROR
            raise
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

    def _run(self, graph_task, input_ds):
        return graph_task.run(input_ds)

    def _make_task_inputs(self, graph_task):
        if not graph_task.dependencies:
            return None

        ds = DataSet()
        for d in reversed(graph_task.dependencies):
            ds.merge(d.output_ds)
        return ds

    def run(self, ds):
        last_ds = ds
        while self.runnable_tasks():
            t = self.runnable_tasks()[0]

            input_ds = self._make_task_inputs(t)
            input_ds = ds if input_ds is None else input_ds

            last_ds = t.run(input_ds)
        return last_ds

    def is_all_completed(self) -> bool:
        return all([gt.status == TaskStatus.COMPLETED for gt in self.graph])


class ConcurrentGraph(Graph):
    def __init__(self, max_workers=None):
        super().__init__()
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def run(self, ds):
        last_ds = ds
        features = []

        while self.is_all_completed() is False:
            tasks = self.runnable_tasks()

            for t in tasks:
                input_ds = self._make_task_inputs(t)
                input_ds = ds if input_ds is None else input_ds

                # タスク内でステータスを変える場合、実際に動き出す前に
                # 再度回ってきて起動する事があるので外部から変更する
                t.status = TaskStatus.RUNNING
                features.append(self.pool.submit(t.run, input_ds))

            # どれかの Feature が終わるまで待つ
            def all_feature_running(features):
                return all([f.running() for f in features])

            while all_feature_running(features):
                import time

                time.sleep(0.1)

            # 終了した features の result を吸い上げ、features を更新
            _next_featrues = []
            for f in features:
                if f.done():
                    last_ds = f.result()
                else:
                    _next_featrues.append(f)
            features = _next_featrues

        return last_ds
