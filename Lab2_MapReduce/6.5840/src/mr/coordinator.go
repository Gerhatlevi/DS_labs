package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	files          []string
	currentFileIdx int
	workingFiles   map[string]int
	numReduce      int
	mapComplete    bool
	reduceComplete bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	if c.currentFileIdx >= len(c.files) {
		// look for reduce jobs if map is all done
		return nil
	}
	// lookup which files we still have left
	reply.workerID = c.currentFileIdx
	reply.fileName = c.files[c.currentFileIdx]
	reply.nReduce = c.numReduce

	c.currentFileIdx = c.currentFileIdx + 1
	return nil
}

func (c *Coordinator) CompleteWork(args *WorkerArgs, reply *WorkerReply) error {
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceComplete
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		currentFileIdx: 0,
		workingFiles:   make(map[string]int),
		numReduce:      nReduce,
		mapComplete:    false,
		reduceComplete: false}

	// the finally spin off the reduce workers to perform the reduce function
	// we essentially need to have a pool of workers that we keep calling
	// the mapf for until all files are processed then we call reduce on all
	// of the workers until all intermediate files are processed

	c.server()
	return &c
}
