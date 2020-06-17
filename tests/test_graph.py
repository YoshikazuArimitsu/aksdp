from aksdp.data import JsonData
from aksdp.dataset import DataSet
from aksdp.task import Task
from aksdp.graph import Graph, TaskStatus
import unittest


class ErrorTask(Task):
    def main(self, ds):
        raise ValueError("ValueError")


class DumbTask(Task):
    def main(self, ds):
        return ds


class TaskA(Task):
    def output_datakeys(self):
        return ["DataA"]

    def main(self, ds):
        return DataSet().put("DataA", JsonData({}))


class TaskB(Task):
    def output_datakeys(self):
        return ["DataB"]

    def main(self, ds):
        return DataSet().put("DataB", JsonData({}))


class TaskC(Task):
    def input_datakeys(self):
        return ["DataA", "DataB"]

    def output_datakeys(self):
        return ["DataC"]

    def main(self, ds):
        return DataSet().put("DataC", JsonData({}))


class TaskD(Task):
    def input_datakeys(self):
        return ["DataA", "DataB"]

    def output_datakeys(self):
        return ["DataA"]

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

    def test_auto_resolver(self):
        g = Graph()
        g.append(TaskA())
        g.append(TaskB())
        taskC = g.append(TaskC())
        g.autoresolve_dependencies()

        self.assertEqual(2, len(taskC.dependencies))

    def test_auto_resolver_error(self):
        g = Graph()
        g.append(TaskA())
        g.append(TaskB())
        g.append(TaskC())
        g.append(TaskD())

        with self.assertRaises(ValueError):
            g.autoresolve_dependencies()

    def test_catalog_ds(self):
        class TestTask(Task):
            def main(self, ds: DataSet):
                if "catalog" not in ds.keys():
                    raise ValueError()
                return DataSet()

        catalog_ds = DataSet().put("catalog", JsonData({}))
        g = Graph(catalog_ds=catalog_ds)
        g.append(TestTask())

        g.run()

    def test_default_ds(self):
        class TestTask(Task):
            def main(self, ds: DataSet):
                if "default" not in ds.keys():
                    raise ValueError()
                return DataSet()

        #
        default_ds = DataSet().put("default", JsonData({}))
        g = Graph()
        g.append(TestTask())
        g.run(default_ds)

    def test_dynamic_graph(self):
        class DynTaskA(Task):
            def __init__(self):
                self._output_datakeys = []

            def output_datakeys(self):
                return self._output_datakeys

            def main(self, ds: DataSet):
                self._output_datakeys = ["DynTaskA"]
                return DataSet()

        class DynTaskB(Task):
            def input_datakeys(self):
                return ["DynTaskA"]

            def main(self, ds: DataSet):
                return DataSet()

        class DynTaskC(Task):
            def input_datakeys(self):
                return ["DynTaskX"]

            def main(self, ds: DataSet):
                return DataSet()

        #
        g = Graph()
        g.append(DynTaskA())
        gtb = g.append(DynTaskB())
        gtc = g.append(DynTaskC())
        g.run()

        self.assertEqual(TaskStatus.COMPLETED, gtb.status)
        self.assertEqual(TaskStatus.INIT, gtc.status)
