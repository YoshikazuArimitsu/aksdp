from aksdp.data import JsonData
from aksdp.repository import LocalFileRepository
from aksdp.task import Task
from aksdp.graph import Graph
import unittest
from pathlib import Path
import os
import tempfile


class ErrorTask(Task):
    def main(self, ds):
        raise ValueError("ValueError")


class DumbTask(Task):
    def main(self, ds):
        return ds


hook_called = 0


class TestGraph(unittest.TestCase):
    def test_no_handler(self):
        g = Graph()
        g.append(ErrorTask())

        with self.assertRaises(ValueError):
            g.run()

    def test_error_handler(self):
        def value_error_handler(e, ds):
            print(str(e))
            print(str(ds))

        g = Graph()
        g.append(ErrorTask())
        g.add_error_handler(ValueError, value_error_handler)
        g.run()

    def test_hook(self):
        g = Graph()
        gt = g.append(DumbTask())

        global hook_called
        hook_called = 0

        def hook(ds):
            global hook_called
            hook_called += 1

        gt.pre_run_hook = hook
        gt.post_run_hook = hook
        g.run()

        self.assertEqual(2, hook_called)
