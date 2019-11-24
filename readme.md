# Overview

The repository houses the code for the CMSC 828D group project: **Implementing a guided forensic analysis tool for a distributed dataflow system**.

# About

Ariadne was a Cretan princess in Greek mythology. She was mostly associated with mazes and labyrinths because of her involvement in the myths of the Minotaur and Theseus.

# Tasks

* taskinstance should get inputs from all parents. all operators should store outputs using run_id & task_id


* create CSV operator to pull the top record off and send into pipeline
* save task metadata to postgres table?
* save resource utilization of task (integrate `os.times`?)