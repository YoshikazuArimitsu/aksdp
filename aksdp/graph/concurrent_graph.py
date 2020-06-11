from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from aksdp.task import Task
from aksdp.dataset import DataSet
from aksdp.graph import Graph, TaskStatus
from typing import List
from enum import Enum
import time
from .graph import GraphTask


logger = getLogger(__name__)


class ConcurrentGraph(Graph):
    def __init__(self, executor=None):
        super().__init__()

        self.pool = executor
        if self.pool is None:
            self.pool = ThreadPoolExecutor()

    def _run(self, graph_task: GraphTask, input_ds: DataSet):
        # ProcessPoolを使用した場合、 GraphTask.run() 内でのアトリビュート更新が効かないので外から操作する
        graph_task.input_ds = input_ds
        graph_task.status = TaskStatus.RUNNING
        return self.pool.submit(graph_task.run, input_ds)

    def run(self, ds: DataSet = None):
        last_ds = ds
        features = []

        while self.is_all_completed() is False and self.abort is False:
            tasks = self.runnable_tasks()

            for t in tasks:
                input_ds = self._make_task_inputs(t)
                input_ds = ds if input_ds is None else input_ds
                features.append((t, self._run(t, input_ds)))

            # どれかの Feature が終わるまで待つ
            def all_feature_running(features):
                return all([f.running() for f in features])

            while all_feature_running([f[1] for f in features]):
                time.sleep(0.1)

            # 終了した features の result を吸い上げ、features を更新
            _next_featrues = []
            for f in features:
                if f[1].done():
                    # ProcessPoolを使用した場合、 GraphTask.run() 内でのアトリビュート更新が効かないので外から操作する
                    try:
                        last_ds = f[0].output_ds = f[1].result()
                        f[0].status = TaskStatus.COMPLETED
                    except BaseException as e:
                        if not self._handle_error(f[0], f[0].input_ds, e):
                            raise
                else:
                    _next_featrues.append(f)
            features = _next_featrues

        return last_ds
