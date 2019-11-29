# Overview

We provide a two part system in order to provide the visualization and analysis of failures within a distributed dataflow system.  In a dataflow system, data (either streaming or batch) can will flow from one node to the next in a dataflow graph of operations.  Each operation will act upon the data in some way.  But failures in a distributed system can be difficult to troubleshoot due to the distributed nature.

This project modified Apache Airflow to act as an idealized dataflow system so that we can build dataflows and gather provenance data during execution.  We then provide the means for analysis and visualization through a web component designed to aid the user resolve failures in the system.

## DataFlow Component Overview

Our dataflow codebase models a standard dataflow type application.  Data items (scalars or vectors) flow from node to node such that the outputs of one node are the inputs for the next.  Per standard dataflow architectures, a node may send its output to multiple children.  Similarly, a child node may get its inputs from multiple parents.  There are branching and other operators to build more complex data workflows.

Along the way, we record execution information and store this in MongoDB.

## UI Component Overview

Our django codebase has been created to assist with analysis of of the dataflow application.  Specifically, we want to provide the means to perform a forensic analysis to troubleshoot errors that occur within the system.  A second (major) goal is to allow the user to follow a suspect/questionable data item as it flows through the system.

### Forensic Analysis

If an error occurs, we want to provide the tools to help diagnose the issue.  This is not just error reporting.  See execution analysis for some related ideas.

### Exection Analysis

Any given data item could flow through the entire system and produce incorrect results without raising any Exceptions.  It would be useful if we could:

* identify when this happens
* follow the changes through the system
* be able to inspect variables at execution time to determine what is happening