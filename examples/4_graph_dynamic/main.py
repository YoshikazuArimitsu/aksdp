import argparse
import random
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from logging import DEBUG, basicConfig, getLogger

from aksdp.dataset import DataSet
from aksdp.graph import ConcurrentGraph, Graph
from aksdp.task import Task
from aksdp.util import PlantUML

logger = getLogger(__name__)


class TaskA(Task):
    def __init__(self):
        self._output_datakeys = []

    def output_datakeys(self):
        return self._output_datakeys

    def main(self, ds):
        logger.info("execute TaskA")
        if random.randint(0, 2) == 0:
            self._output_datakeys = ["DataA-1"]
        else:
            self._output_datakeys = ["DataA-2"]
        return DataSet()


class TaskB(Task):
    def input_datakeys(self):
        return ["DataA-1"]

    def main(self, ds):
        logger.info("execute TaskB")
        return DataSet()


class TaskC(Task):
    def input_datakeys(self):
        return ["DataA-2"]

    def main(self, ds):
        logger.info("execute TaskC")
        return DataSet()


if __name__ == "__main__":
    basicConfig(level=DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--graph", type=str, default="normal", help="Graph {normal/thread/process}")
    args = parser.parse_args()

    if args.graph == "normal":
        graph = Graph()
        logger.info("use Graph")
    elif args.graph == "thread":
        graph = ConcurrentGraph(ThreadPoolExecutor())
        logger.info("use ConcurrentGraph(ThreadPoolExecutor)")
    elif args.graph == "process":
        graph = ConcurrentGraph(ProcessPoolExecutor())
        logger.info("use ConcurrentGraph(ProcessPoolExecutor)")
    else:
        logger.error("unknown graph,")
        sys.exit(-1)

    # Graphで処理する
    graph.append(TaskA())
    graph.append(TaskB())
    graph.append(TaskC())
    graph.run()
    print(f"PlantUML Diagraph : {PlantUML.graph_to_url(graph)}")
