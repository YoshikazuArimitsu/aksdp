import os
from pathlib import Path
from logging import DEBUG, basicConfig, getLogger

from aksdp.task import Task
from aksdp.util import PlantUML
from aksdp.util import graph_factory as gf

logger = getLogger(__name__)


class TaskA(Task):
    def __init__(self, params):
        super().__init__(params)

    def main(self, ds):
        logger.info(f"run TaskA, params={self.params}")
        return ds


class TaskB(Task):
    def main(self, ds):
        logger.info(f"run TaskB, params={self.params}")
        return ds


class TaskC(Task):
    def main(self, ds):
        logger.info(f"run TaskC, params={self.params}")
        return ds


class TaskD(Task):
    def main(self, ds):
        logger.info(f"run TaskD, params={self.params}")
        return ds


if __name__ == "__main__":
    basicConfig(level=DEBUG)

    config = Path(os.path.dirname(__file__)) / Path("graph.yml")
    graph = gf.create_from_file(config)

    graph.run()
    print(f"PlantUML Diagraph : {PlantUML.graph_to_url(graph)}")
