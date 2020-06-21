from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from aksdp.graph import Graph, TaskStatus, GraphTask
from aksdp.dataset import DataSet
import time


logger = getLogger(__name__)


class ConcurrentGraph(Graph):
    def __init__(self, executor=None, catalog_ds: DataSet = DataSet(), disable_dynamic_dep: bool = False):
        super().__init__(catalog_ds, disable_dynamic_dep=disable_dynamic_dep)

        self.pool = executor
        if self.pool is None:
            self.pool = ThreadPoolExecutor()

    def _run(self, graph_task: GraphTask, input_ds: DataSet):
        # ProcessPoolを使用した場合、 GraphTask.run() 内でのアトリビュート更新が効かないので外から操作する
        graph_task.input_ds = input_ds
        graph_task.status = TaskStatus.RUNNING
        r = self.pool.submit(graph_task.run, input_ds)
        return r

    def run(self, ds: DataSet = None):
        last_ds = ds
        self.abort = False
        features = []

        while not self.abort:
            tasks = self.runnable_tasks()

            for t in tasks:
                input_ds = self._make_task_inputs(t, ds)
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
                    try:
                        # 新しく返った GraphTask で差し替え
                        rgt = f[1].result()
                        self.graph[self.graph.index(f[0])] = rgt
                        last_ds = rgt.output_ds
                    except BaseException as e:
                        if not self._handle_error(f[0], f[0].input_ds, e):
                            raise
                else:
                    _next_featrues.append(f)
            features = _next_featrues

            if not features and not self.runnable_tasks():
                logger.debug("no runnables tasks, exit")
                break

        return last_ds
