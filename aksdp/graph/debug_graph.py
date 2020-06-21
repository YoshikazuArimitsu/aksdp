from aksdp.graph import Graph, GraphTask
from aksdp.data import DataType
from aksdp.dataset import DataSet
from aksdp.repository import LocalFileRepository
from pathlib import Path
from logging import getLogger
import shutil
import pickle
import os

logger = getLogger(__name__)


class DebugGraph(Graph):
    """全タスクに実行前/後フックを差し込んで入出力を保存するGraph
    """

    def __init__(self, base_dir: Path, catalog_ds: DataSet = DataSet()):
        super().__init__(catalog_ds)
        logger.info(f"set up DebugGraph, base_dir={base_dir.name}")
        self.base_dir = base_dir

    @classmethod
    def save_ds_hook(cls, base_dir: Path, task_name: str):
        def dump_dataset(_base_dir, _task_name, ds):
            shutil.rmtree(_base_dir, ignore_errors=True)
            os.makedirs(_base_dir, exist_ok=True)

            # pickle DataSet
            with open(_base_dir / f"{_task_name}.pkl", "wb") as f:
                pickle.dump(ds, f)

            # dump DataSet data
            if ds:
                for k in ds.keys():
                    # 一瞬 Repository を差し替えてダンプ先に保存する
                    d = ds.get(k)
                    original_repo = d.repository

                    ext = ""
                    if d.data_type == DataType.DATAFRAME:
                        ext = ".csv"
                    if d.data_type == DataType.JSON:
                        ext = ".json"

                    dump_repo = LocalFileRepository(_base_dir / (k + ext))
                    d.repository = dump_repo

                    try:
                        d.save()
                    except BaseException as e:
                        logger.warning(f"dump failed, {str(e)}")

                    d.repository = original_repo

        def _save_ds(ds):
            dump_dataset(base_dir, task_name, ds)

        return _save_ds

    def append(self, task, dependencies=None):
        gt = super().append(task, dependencies)
        cn = task.__class__.__name__
        gt.pre_run_hook = DebugGraph.save_ds_hook(self.base_dir / Path(cn) / "in", cn)
        gt.post_run_hook = DebugGraph.save_ds_hook(self.base_dir / Path(cn) / "out", cn)
        return gt

    def run_task(self, gt: GraphTask):
        cn = gt.task.__class__.__name__
        path = self.base_dir / Path(cn) / "in" / f"{cn}.pkl"

        try:
            with open(path, "rb") as f:
                pkl = pickle.load(f)
        except BaseException as e:
            logger.error(f"can't load pickle, {str(e)}")
            return

        return gt.task.gmain(pkl)
