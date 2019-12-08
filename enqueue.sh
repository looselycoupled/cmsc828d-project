#!/bin/bash

# trigger any dag an arbitrary number of times
for run in {1..15}
do
  airflow trigger_dag "test-random-error"
done



