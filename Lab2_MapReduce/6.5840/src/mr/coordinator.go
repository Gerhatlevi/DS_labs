package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const DEFAULT_COORDINATOR_RPC_PORT = "1234"

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type WorkerData struct {
	IpAddr       string
	IpPort       int
	ProgressRate float64
	TaskIDs      []int
}

type Task struct {
	Status    TaskStatus
	FileName  string // only for map tasks
	WorkerID  string
	StartTime time.Time
}

type Coordinator struct {
	// since every RPC is a different goroutine we have to make sure at one given time only 1 R/W the tasks arrays
	mu                   sync.Mutex
	workers              map[string]WorkerData
	mapTasks             []Task
	reduceTasks          []Task
	completedReduceCount int
	nMap                 int
	nReduce              int
	allDone              bool
	servePort            int
}

func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid := uuid.New().String()
	c.workers[uuid] = WorkerData{
		IpAddr:       args.IpAddress,
		IpPort:       args.Port,
		TaskIDs:      []int{},
		ProgressRate: 0,
	}

	// worker should use this uuid for reporting status information
	reply.Uuid = uuid
	log.Printf("new uuid: %s, ip: %s, port: %d", uuid, args.IpAddress, args.Port)
	return nil
}

// The RecomputeWork RPC call is used for workers to report when another worker is unreachable
// particularly for the reduce phase when a worker needs to collect the intermediate data files
// and the worker host is unreachable. This assumes that the worker has gone down and the work
// that was completed on that node must then be recomputed.
func (c *Coordinator) RecomputeWork(args *RecomputeWorkArgs, reply *RecomputeWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// simply go through the tasks that were assigned for the reported worker that was unreachable
	// and set those tasks to IDLE and let the coordinator go through the normal redistribution of work
	// NOTE: we only need to recompute the map tasks here, reduce tasks will give the coordinator
	//    back its final computed value so we don't care if the workerID is out of sync
	foundJobsToRecompute := false
	for i, task := range c.mapTasks {
		if task.WorkerID == args.WorkerID {
			c.mapTasks[i].Status = Idle
			c.mapTasks[i].WorkerID = ""
			foundJobsToRecompute = true
		}
	}
	// if a worker requested this RPC, we should have found jobs to recompute here and assume that the
	// workerID is no longer reachable, so clear this information from the database
	reply.Success = foundJobsToRecompute
	delete(c.workers, args.WorkerID)
	return nil
}

func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if we have any map jobs first that need to be completed
	allMapsDone := true
	for i := range c.mapTasks {
		mapTask := &c.mapTasks[i]
		if mapTask.Status != Completed {
			allMapsDone = false
		}
		if mapTask.Status == Idle ||
			(mapTask.Status == InProgress && time.Since(mapTask.StartTime) > 10*time.Second) {

			mapTask.Status = InProgress
			mapTask.WorkerID = args.WorkerID
			mapTask.StartTime = time.Now()

			// update the reply to the correct data for the worker job
			reply.TaskType = MapTask
			reply.TaskID = i
			// NOTE: we assume that all workers also have local access to the input
			// corpus files since that is how MapReduce is implemented in the paper
			// where input files reside locally and the coordinator schedules jobs
			// based on that knowledge of where these input files live
			reply.FileName = c.mapTasks[i].FileName
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
		reduceTask := &c.reduceTasks[i]
		if reduceTask.Status != Completed {
			allReducesDone = false
		}
		if reduceTask.Status == Idle ||
			(reduceTask.Status == InProgress && time.Since(reduceTask.StartTime) > 10*time.Second) {

			reduceTask.Status = InProgress
			reduceTask.WorkerID = args.WorkerID
			reduceTask.StartTime = time.Now()

			reply.TaskType = ReduceTask
			reply.TaskID = i
			reply.NMap = c.nMap

			// send the file locations to this reduce task to collect intermediate data
			fileLocations := make(map[int]FileLocation)
			for i, t := range c.mapTasks {
				workerData := c.workers[t.WorkerID]
				fileLocations[i] = FileLocation{
					Address: workerData.IpAddr,
					Port:    workerData.IpPort,
				}
			}
			reply.FileLocations = fileLocations
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

	// we ignore any completion messages if we already recorded this task as complete
	if task.Status == Completed {
		return nil
	}

	if err := moveStatus(task); err != nil {
		log.Print(err)
		return err
	}

	// update the task with the worker id that completed the work.
	// it should usually be the one that was initially assigned, but we sometimes kick
	// off backup jobs or recompute
	task.WorkerID = args.WorkerID
	if args.TaskType == ReduceTask {
		// write the final reduce output data to disk
		out, err := os.Create(fmt.Sprintf("mr-out-%d", args.TaskID))
		defer out.Close()

		if err != nil {
			return err
		}

		if _, err := out.Write(args.ReduceTaskData); err != nil {
			return err
		}
		if err := out.Sync(); err != nil {
			return err
		}

		c.completedReduceCount += 1
	}

	// check if this was the last reduce task we were waiting on
	if c.completedReduceCount == len(c.reduceTasks) {
		c.allDone = true
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
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", c.servePort))
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
	rpcEnvVar := os.Getenv("RPC_PORT")
	if rpcEnvVar == "" {
		rpcEnvVar = DEFAULT_COORDINATOR_RPC_PORT
	}
	servePort, err := strconv.Atoi(rpcEnvVar)
	if err != nil {
		log.Fatal("failed to parse environment var RPC_PORT, should be valid port example: RPC_PORT=1234")
	}

	c := Coordinator{
		workers:              make(map[string]WorkerData),
		mapTasks:             make([]Task, len(files)),
		reduceTasks:          make([]Task, nReduce),
		completedReduceCount: 0,
		nMap:                 len(files),
		nReduce:              nReduce,
		allDone:              false,
		servePort:            servePort,
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
