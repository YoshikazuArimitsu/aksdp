from aksdp.util import graph_factory as gf
from aksdp.task import Task
import unittest


class TaskA(Task):
    def __init__(self, params):
        super().__init__(params)

    def main(self, ds):
        return ds


class TaskB(Task):
    def main(self, ds):
        return ds


class TestGraphFactory(unittest.TestCase):
    def test_create_1(self):
        gd = {
            "graph": {"class": "Graph", "base_dir": "."},
            "tasks": [
                {"name": "taska", "class": "test_graph_factory.TaskA", "params": {"key": "value"}},
                {
                    "name": "taskb",
                    "class": "test_graph_factory.TaskB",
                    "params": {"key": "value"},
                    "dependencies": ["taska"],
                },
            ],
        }
        graph = gf.create_from_dict(gd)
        graph.run()

    def test_create_error_1(self):
        """依存を解決できないパターン"""
        gd = {
            "graph": {"class": "Graph", "base_dir": "."},
            "tasks": [
                {"name": "taska", "class": "test_graph_factory.TaskA", "params": {"key": "value"}},
                {
                    "name": "taskb",
                    "class": "test_graph_factory.TaskB",
                    "params": {"key": "value"},
                    "dependencies": ["taskx"],
                },
            ],
        }

        with self.assertRaises(ValueError):
            gf.create_from_dict(gd)
