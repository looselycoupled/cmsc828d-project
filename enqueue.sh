#!/bin/bash

# trigger any dag an arbitrary number of times
for run in {1..50}
do
  airflow trigger_dag "test-multi-parent"
done
