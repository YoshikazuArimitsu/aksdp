import time
from logging import getLogger

from aksdp.graph import Graph, TaskStatus

from airflow import DAG

logger = getLogger(__name__)


class AirFlow(object):
    def __init__(self, g: Graph):
        self.graph = g

    def to_dag(self, dag: DAG):
        pass
