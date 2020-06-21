from logging import getLogger
from aksdp.task import Task
from aksdp.dataset import DataSet
from typing import List, Callable
from .graph_task import GraphTask, TaskStatus

logger = getLogger(__name__)


class Graph:
    def __init__(self, catalog_ds: DataSet = None, disable_dynamic_dep: bool = False):
        """.ctor
        """
        self.graph = []
        self.error_handlers = []
        self.abort = False
        self.catalog_ds = catalog_ds if catalog_ds else DataSet()
        self.disable_dynamic_dep = disable_dynamic_dep

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

    def _run(self, graph_task: GraphTask, input_ds: DataSet) -> DataSet:
        """タスクの起動

        Args:
            graph_task (GraphTask): 起動するタスク
            input_ds (DataSet): 入力DataSet

        Returns:
            [DataSet]: 出力DataSet
        """

        try:
            rt = graph_task.run(input_ds)
            return rt.output_ds
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
        self.abort = False

        while not self.abort:
            if not self.runnable_tasks():
                break

            t = self.runnable_tasks()[0]

            input_ds = self._make_task_inputs(t, ds)
            last_ds = self._run(t, input_ds)

        return last_ds

    def runnable_tasks(self) -> List[GraphTask]:
        """実行可能タスクの取得

        Returns:
            List[GraphTask]: 実行可能タスク
        """
        if self.disable_dynamic_dep:
            # 動的依存解決無効
            return [g for g in self.graph if g.is_runnable()]

        # 動的依存解決
        runnable_tasks = []
        init_tasks = [g for g in self.graph if g.status == TaskStatus.INIT]
        completed_tasks = [g for g in self.graph if g.status == TaskStatus.COMPLETED]

        def _find_datakey_provider(key: str) -> List[GraphTask]:
            return [gt for gt in completed_tasks if key in gt.task.output_datakeys()]

        for gt in init_tasks:
            deps = set()
            for ik in gt.task.input_datakeys():
                if ik in self.catalog_ds.keys():
                    continue

                prv = _find_datakey_provider(ik)

                if len(prv) != 1:
                    # logger.debug(f"data({ik}) provide from {len(prv)} tasks.")
                    break

                deps.add(prv[0])
            else:
                gt.dependencies_dynamic = list(deps)

                if gt.is_runnable():
                    runnable_tasks.append(gt)

        return runnable_tasks

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

            gt.dependencies_dynamic = list(deps)

        logger.info("task autoresolve completed.")
