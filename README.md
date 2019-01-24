## Project 4 of CS6210 in Gatech.

This project implemented a simplied version of `MapReduce` infrastructure.

## About MapReduce

MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines. The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication.

## Group Member

Group Member: Haoran Li

Submission Date: Dec. 3, 2018

##Start-up Instruction
To compile the source code and run the experiment, enter the following code in terminal

- Goto src directory and run `make` command.
  - Two libraries would be created in external directory: `libmapreduce.a` and `libmr_worker.a`.
  - Now goto test directory and run `make` command.
  - Two binaries would be created: `mrdemo` and `mr_worker`.
  - **Now running the demo, once you have created all the binaries and libraries.**
   1. Clear the files if any in the output directory
   2. Start all the worker processes in this fashion(e.g. for 2 workers): `./mr_worker localhost:50051 & ./mr_worker localhost:50052;`
   3. Then start your main map reduce process: `./mrdemo config.ini`
   4. Once the ./mrdemo finishes, kill all the worker proccesses you started.
   5. Check output directory to see if you have the correct results (obviusly once you have done the proper implementation of your library)

## Project Assignment Files

[Project Description](description.md)

[Code walk through](structure.md)

[How to setup the project](INSTALL.md)
