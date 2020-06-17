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
        """実行中かどうか

        Returns:
            bool: 実行中
        """
        if self.status != TaskStatus.INIT:
            return False

        if self.dependencies is None:
            return True

        return all([gt.status == TaskStatus.COMPLETED for gt in self.dependencies])

    def run(self, ds: DataSet = None) -> DataSet:
        """タスク実行

        Args:
            ds (DataSet, optional): 入力DataSet. Defaults to None.

        Returns:
            DataSet: 出力DataSet
        """
        try:
            self.input_ds = ds
            logger.debug(f"task({self.task.__class__.__name__}) started.")
            logger.debug(f"  input_ds = {str(ds)}")
            self.pre_run_hook(ds)
            output_ds = self.task.gmain(ds)
            self.post_run_hook(output_ds)

            logger.debug(f"task({self.task.__class__.__name__}) completed. (elapse={self.task.elapsed_time:.3f}s)")
            logger.debug(f"  output_ds = {str(output_ds)}")
            self.output_ds = output_ds
        except BaseException as e:
            logger.error(f"task({self.task.__class__.__name__}) failed. {str(e)}")
            raise
        return self.output_ds


class Graph:
    def __init__(self, catalog_ds: DataSet = None):
        """.ctor
        """
        self.graph = []
        self.error_handlers = []
        self.abort = False
        self.catalog_ds = catalog_ds

    def append(self, task: Task, dependencies: List[GraphTask] = []) -> GraphTask:
        """Taskの追加

        Args:
            task (Task): Taskインスタンス
            dependencies (List[GraphTask], optional): 依存タスク. Defaults to [].

        Returns:
            GraphTask: GraphTask
        """
        gt = GraphTask(task, dependencies)
        self.graph.append(gt)
        return gt

    def add_error_handler(self, cls, fn: Callable):
        """エラーハンドラの追加

        Args:
            cls (class): 例外の型
            fn (function): エラーハンドラのCallable
        """
        self.error_handlers.append((cls, fn))

    def runnable_tasks(self) -> List[GraphTask]:
        return [g for g in self.graph if g.is_runnable()]

    def _run(self, graph_task: GraphTask, input_ds: DataSet) -> DataSet:
        """タスクの起動

        Args:
            graph_task (GraphTask): 起動するタスク
            input_ds (DataSet): 入力DataSet

        Returns:
            [DataSet]: 出力DataSet
        """

        try:
            graph_task.status = TaskStatus.RUNNING
            r = graph_task.run(input_ds)
            graph_task.status = TaskStatus.COMPLETED
            return r
        except BaseException as e:
            if not self._handle_error(graph_task, input_ds, e):
                raise

    def _handle_error(self, graph_task: GraphTask, input_ds: DataSet, e: BaseException) -> bool:
        """エラー時の処理。
        対応するエラーハンドラが存在すればエラーハンドラ呼び出し

        Args:
            graph_task (GraphTask): エラーが発生したタスク
            input_ds (DataSet): 入力データセット
            e (BaseException): 例外インスタンス

        Returns:
            bool: エラーハンドリングしたかどうか
        """
        graph_task.status = TaskStatus.ERROR
        for eh in self.error_handlers:
            if isinstance(e, eh[0]):
                self.abort = True
                eh[1](e, input_ds)
                return True

        return False

    def _make_task_inputs(self, graph_task: GraphTask, default_ds: DataSet) -> DataSet:
        """タスクの入力DataSet作成

        Args:
            graph_task (Task): GraphTask
            default_ds (DataSet): デフォルトDataSet

        Returns:
            DataSet: 入力DataSet
        """
        ds = DataSet()
        ds.merge(self.catalog_ds)

        if not graph_task.dependencies:
            ds.merge(default_ds)
        else:
            for d in graph_task.dependencies:
                ds.merge(d.output_ds)
        return ds

    def run(self, ds: DataSet = None) -> DataSet:
        last_ds = ds
        while self.runnable_tasks() and self.abort is False:
            t = self.runnable_tasks()[0]

            input_ds = self._make_task_inputs(t, ds)

            last_ds = self._run(t, input_ds)
        return last_ds

    def is_all_completed(self) -> bool:
        return all([gt.status == TaskStatus.COMPLETED for gt in self.graph])

    def autoresolve_dependencies(self):
        """グラフ依存関係の自動解決
        """
        logger.info("run task dependencies auto resolver...")

        def _find_datakey_provider(key: str) -> List[GraphTask]:
            return [gt for gt in self.graph if key in gt.task.output_datakeys()]

        for gt in self.graph:
            deps = set()
            for ik in gt.task.input_datakeys():
                prv = _find_datakey_provider(ik)
                prv_str = ",".join([gt.task.__class__.__name__ for gt in prv])

                if len(prv) > 1:
                    msg = f"data({ik}) provide from multiple task({prv_str})"
                    logger.error(msg)
                    raise ValueError(msg)

                if len(prv) == 0 and ik not in self.catalog_ds.keys():
                    # input を提供するタスクが無く、カタログにも同キーが無い場合はエラー
                    msg = f"task provide data({ik}) not found."
                    logger.info(msg)

                logger.debug(f"{gt.task.__class__.__name__} : depend on {prv[0].task.__class__.__name__} by data({ik})")
                deps.add(prv[0])

            gt.dependencies = list(deps)

        logger.info("task autoresolve completed.")
