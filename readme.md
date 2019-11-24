# Overview

The repository houses the code for the CMSC 828D group project: **Implementing a guided forensic analysis tool for a distributed dataflow system**.

## Notes

Operators have been converted too look like streaming dataflow nodes.  Inputs are taken from redis using parent task names and outputs are stored in redis using current task name (plus dagrun.run_id).

All DAGs should disable retries to limit taskinstance.try_number to 1.

## Project TODOs

* store after-task metadata inside dagrun json object rather than as separate mongodb objects
* save resource utilization of task (integrate `os.times`?)





1. task should get dagrun metadata from mongodb.  if not available then create new document.

```python
document = {
    "dag": dag_id,
    "execution_time": "",
    "run_id": "",
    "task_executions_": [
        {
            "task_id": ti.task.task_id,
            "try_number": ti.try_number,
            "inflow": None,
            "outflow": None,
            "resource_utilization": {
                "cpu": [],
            },
            "total_execution_time": None,
        },
    ]

}
```


## About

Ariadne was a Cretan princess in Greek mythology. She was mostly associated with mazes and labyrinths because of her involvement in the myths of the Minotaur and Theseus.

