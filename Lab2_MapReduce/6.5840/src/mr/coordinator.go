package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type WorkerData struct {
	ipAddr       string
	ipPort       int
	progressRate float64
}

type Task struct {
	Status       TaskStatus
	FileName     string // only for map tasks
	LocationIP   string
	LocationPort int
	StartTime    time.Time
}

type Coordinator struct {
	// since every RPC is a different goroutine we have to make sure at one given time only 1 R/W the tasks arrays
	mu          sync.Mutex
	workers     map[string]WorkerData
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
	allDone     bool
}

func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid := uuid.New().String()
	c.workers[uuid] = WorkerData{
		ipAddr:       args.IpAddress,
		ipPort:       args.Port,
		progressRate: 0,
	}

	// worker should use this uuid for reporting status information
	reply.Uuid = uuid
	log.Printf("new uuid: %s, ip: %s, port: %d", uuid, args.IpAddress, args.Port)
	return nil
}

func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if we have any map jobs first that need to be completed
	allMapsDone := true
	for i := range c.mapTasks {
		if c.mapTasks[i].Status != Completed {
			allMapsDone = false
		}
		if c.mapTasks[i].Status == Idle ||
			(c.mapTasks[i].Status == InProgress && time.Since(c.mapTasks[i].StartTime) > 10*time.Second) {

			c.mapTasks[i].Status = InProgress
			c.mapTasks[i].StartTime = time.Now()

			// update the reply to the correct data for the worker job
			reply.TaskType = MapTask
			reply.TaskID = i
			reply.FileName = c.mapTasks[i].FileName
			// we are reading and replying with the full file data here for the worker
			if content, err := readFile(reply.FileName); err == nil {
				reply.FileData = content
			}
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	// make the worker wait if we are still working on the map phase
	if !allMapsDone {
		reply.TaskType = WaitTask
		return nil
	}

	// start giving out the reduce jobs since we know the map stage is complete
	allReducesDone := true
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status != Completed {
			allReducesDone = false
		}
		if c.reduceTasks[i].Status == Idle ||
			(c.reduceTasks[i].Status == InProgress && time.Since(c.reduceTasks[i].StartTime) > 10*time.Second) {

			c.reduceTasks[i].Status = InProgress
			c.reduceTasks[i].StartTime = time.Now()

			reply.TaskType = ReduceTask
			reply.TaskID = i
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	// make the worker wait in case we need to kick off a backup job for a slow or failed reduce worker
	if !allReducesDone {
		reply.TaskType = WaitTask
		return nil
	}

	c.allDone = true
	reply.TaskType = ExitTask
	return nil
}

// moveStatus is a helper function to move the status through the state
// machine and report errors for unexpected results
func moveStatus(task *Task) error {
	switch task.Status {
	case InProgress:
		task.Status = Completed
	case Idle:
		return errors.New("cannot complete a non started task")
	}
	return nil
}

func (c *Coordinator) CompleteWork(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	switch args.TaskType {
	// need to check because it can happen that slower tasks finished after a faster completed that beforehand
	case MapTask:
		task = &c.mapTasks[args.TaskID]
	case ReduceTask:
		task = &c.reduceTasks[args.TaskID]
	default:
		return errors.New("task type not supported")
	}

	if err := moveStatus(task); err != nil {
		log.Print(err)
		return err
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// TODO: also keep track here for connections to tell other workers
	// where to go to collect intermediate jobs
	c := Coordinator{
		workers:     make(map[string]WorkerData),
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nMap:        len(files),
		nReduce:     nReduce,
		allDone:     false,
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			Status:   Idle,
			FileName: file,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Status: Idle,
		}
	}

	c.server()
	return &c
}
