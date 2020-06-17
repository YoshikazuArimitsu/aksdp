import functools
from logging import getLogger
from pprint import pprint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from aksdp.dataset import DataSet
from aksdp.graph import Graph

logger = getLogger(__name__)


def af_entrypoint(gt, ds, **kwargs):
    logger.info(f"ds={ds}")
    pprint(kwargs)

    # build Input DataSet from Xcoms
    ti = kwargs["ti"]
    ds = DataSet()
    for d in gt.dependencies:
        cn = d.task.__class__.__name__

        pull_ds = ti.xcom_pull(task_ids=cn)
        if pull_ds:
            logger.info(f"found Xcom from {cn}")
            ds.merge(pull_ds)

    # Run & retrun Xcoms
    return gt.task.main(ds)


class AirFlow(object):
    def __init__(self, g: Graph):
        self.graph = g
        self.dag_tasks = {}

    def to_dag(self, dag: DAG):
        logger.info("Task regist to dag...")
        # create operators
        for gt in self.graph.graph:
            cn = gt.task.__class__.__name__
            func = functools.partial(af_entrypoint, gt)
            op = PythonOperator(task_id=f"{cn}", provide_context=True, python_callable=func, dag=dag)
            logger.info(f"create airflow operator {cn}")
            self.dag_tasks[cn] = op

        # set dependencies
        for gt in self.graph.graph:
            cn = gt.task.__class__.__name__
            op = self.dag_tasks[cn]

            for dep in gt.dependencies:
                dep_cn = dep.task.__class__.__name__
                op_ups = self.dag_tasks[dep_cn]

                op.set_upstream(op_ups)
                logger.info(f"{cn} set_upstream to {dep_cn}")
