from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger
from aksdp.task import Task
from aksdp.dataset import DataSet
from aksdp.graph import Graph, TaskStatus
from typing import List
from enum import Enum
import time


logger = getLogger(__name__)


class ConcurrentGraph(Graph):
    def __init__(self, max_workers=None):
        super().__init__()
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def run(self, ds: DataSet = None):
        last_ds = ds
        features = []

        while self.is_all_completed() is False and self.abort is False:
            tasks = self.runnable_tasks()

            for t in tasks:
                input_ds = self._make_task_inputs(t)
                input_ds = ds if input_ds is None else input_ds

                # タスク内でステータスを変える場合、実際に動き出す前に
                # 再度回ってきて起動する事があるので外部から変更する
                t.status = TaskStatus.RUNNING
                features.append(self.pool.submit(self._run, t, input_ds))

            # どれかの Feature が終わるまで待つ
            def all_feature_running(features):
                return all([f.running() for f in features])

            while all_feature_running(features):
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
