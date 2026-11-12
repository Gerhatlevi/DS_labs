## TDA596 Distributed Systems Labs

This repository holds all of the labs that were completed as a part of the Distributed Systems course
at Chalmers. Each of the labs are described in more detail below. Also note that each of the specific
lab details can be found in a README.md under each of the respective lab folders.

### Lab 1: HTTP Server and Pub-Sub Broker

Intro to go lab that introduces basic socket programming in go. This implements an HTTP server that handles
connections by spawning child processes. Then a pub-sub broker is added to handle subscribing and publishing
to different topics.

### Lab 2: MapReduce

More advanced lab that implements the concept of MapReduce from the
[google research paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).
This lab calls application Map and Reduce functions and also a main coordinator process that manages scheduling
tasks to workers and dealing with failed workers.
