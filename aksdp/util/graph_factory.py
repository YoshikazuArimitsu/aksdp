import yaml
import json
from pathlib import Path
from logging import getLogger
from aksdp.graph import Graph, ConcurrentGraph, DebugGraph
from aksdp.task import Task
from typing import List
import importlib


logger = getLogger(__name__)

"""
includes:
    - xxx.yaml
graph:
    class: Graph/ConcurrentGraph/DebugGraph
    base_dir: DebugGraph only

tasks:
  - name:
    class:
    params:
    dependencies:
      - 
  - class:
    params:

"""


def create_graph(config: dict) -> Graph:
    clazz = config.get("class")
    if clazz == "ConcurrentGraph":
        return ConcurrentGraph()
    elif clazz == "DebugGraph":
        return DebugGraph(Path(config.get("base_dir")))
    return Graph()


def create_tasks(task_config: list) -> List[Task]:
    tasks = {}

    for i, c in enumerate(task_config):
        params = c.get("params", {})
        cs = c.get("class").split(".")
        mod = ".".join(cs[:-1])
        _cls = cs[-1]

        if mod:
            m = importlib.import_module(mod)
            ctor = getattr(m, _cls)
        else:
            ctor = globals()[_cls]
        inst = ctor(params)

        name = c.get("name", f"task_{i}")
        dependencies = c.get("dependencies", [])

        tasks[name] = {"instance": inst, "dependencies": dependencies}
    return tasks


def add_tasks_to_graph(graph: Graph, tasks: list):
    added_tasks = {}

    def exist_all_dpes(at, t):
        ds = t["dependencies"]
        return all([d in at for d in ds])

    def all_deps(at, t):
        ds = t["dependencies"]
        deps = [at[d] for d in ds]
        return deps

    while len(added_tasks) < len(tasks):
        for k, t in tasks.items():
            if k in added_tasks:
                # 追加済
                continue

            # 追加可能なら追加
            if exist_all_dpes(added_tasks, t):
                gt = graph.append(t["instance"], all_deps(added_tasks, t))
                added_tasks[k] = gt
                break
        else:
            # 依存を解決できないタスクがあった
            for k, t in tasks.items():
                if k not in added_tasks:
                    logger.error(f"{t['instance'].__class__.__name__} can't resolve dependencies")
            raise ValueError("build graph failed")


def create_from_dict(config: dict) -> Graph:
    graph = create_graph(config.get("graph"))
    tasks = create_tasks(config.get("tasks", []))
    add_tasks_to_graph(graph, tasks)
    return graph

def _load(path: Path) -> dict:
    try:
        with open(path, "r") as f:
            try:
                d = yaml.load(f, Loader=yaml.FullLoader)
                return d
            except:
                d = json.load(f)
                return d
    except BaseException as ex:
        logger.error(f"load failed, {str(ex)}")
        raise

def _merge(d1: dict, d2:dict) -> dict:
    r = {}

    # graph d2優先
    if "graph" in d2.keys():
        r["graph"] = d2["graph"]
    else:
        r["graph"] = d1.get("graph")

    # tasks マージ
    ts1 = d1.get("tasks", [])
    ts2 = d2.get("tasks", [])
    ts1.extend(ts2)
    r["tasks"] = ts1
    return r


def _load_recursive(path: Path, config:dict = {}) -> dict:
    d = _load(path)
    parent = path.parent

    if d.get("includes"):
        includes = d["includes"]
        includes_dict = [_load_recursive(parent / Path(i)) for i in includes]

        inc = {}
        for i in includes_dict:
            inc = _merge(inc, i)

        d = _merge(inc, d)
    return d


def create_from_file(path: Path) -> Graph:
    config = _load_recursive(path)
    return create_from_dict(config)
