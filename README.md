# aksdp

## Overview

A simple framework for writing data pipelines in Python

## INSTALL

```bash
$ pip install git+https://github.com/YoshikazuArimitsu/aksdp.git
```

## QuickStart

```python

class TaskA(Task):
   def main(self, ds):
      return ds

class TaskB(Task):
   ...

class TaskC(Task):
   ...

class TaskD(Task):
   ...

graph = Graph()
task_a = graph.append(TaskA())
task_b = graph.append(TaskB(), [task_a])
task_c = graph.append(TaskC(), [task_b])
task_d = graph.append(TaskD(), [task_b, task_c])
graph.run()

```

Each task runs after each dependent task completes.  
Also, the data output upstream can be received as input data and processed.