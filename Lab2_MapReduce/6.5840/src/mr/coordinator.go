package mr

import (
	"encoding/json"
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
const SNAPSHOT_FREQUENCY_SECONDS = 2

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

func (w *WorkerData) FormatAddress() string {
	return fmt.Sprintf("%s:%d", w.IpAddr, w.IpPort)
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
	Workers              map[string]WorkerData
	MapTasks             []Task
	ReduceTasks          []Task
	CompletedReduceCount int
	NumMap               int
	NumReduce            int
	AllDone              bool
	ServePort            int
}

const coordinatorCheckpointFile = "Coordinator.checkpoint"

// SaveCoordinatorCheckpoint is used for periodically writing the Coordinator state c to disk
// which can then be used in LoadCoordinatorCheckpoint for loading from checkpoint. The format
// of the data is just simply writing to json data since the object can be easily marshaled to JSON
func (c *Coordinator) SaveCoordinatorCheckpoint() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Print("Checkpointing the Coordinator State")
	// NOTE: it's okay that we rewrite this file because we need to update the entire object
	checkpointFile, err := os.Create(coordinatorCheckpointFile)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(checkpointFile)
	if err := encoder.Encode(c); err != nil {
		return err
	}

	checkpointFile.Close()
	return nil
}

func (c *Coordinator) LoadCoordinatorCheckpoint() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	checkpointFile, err := os.Open(coordinatorCheckpointFile)
	if err != nil {
		return err
	}

	encoder := json.NewDecoder(checkpointFile)
	encoder.Decode(c)

	return nil
}

func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid := uuid.New().String()
	c.Workers[uuid] = WorkerData{
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
func (c *Coordinator) RecomputeWork(args *RecomputeWorkArgs, reply *SuccessReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// simply go through the tasks that were assigned for the reported worker that was unreachable
	// and set those tasks to IDLE and let the coordinator go through the normal redistribution of work
	// NOTE: we only need to recompute the map tasks here, reduce tasks will give the coordinator
	//    back its final computed value so we don't care if the workerID is out of sync
	foundJobsToRecompute := false
	for i, task := range c.MapTasks {
		if task.WorkerID == args.WorkerID {
			c.MapTasks[i].Status = Idle
			c.MapTasks[i].WorkerID = ""
			foundJobsToRecompute = true
		}
	}
	// if a worker requested this RPC, we should have found jobs to recompute here and assume that the
	// workerID is no longer reachable, so clear this information from the database
	reply.Success = foundJobsToRecompute
	delete(c.Workers, args.WorkerID)
	return nil
}

// GetWork is used as the primary RPC call for workers to get either Map or Reduce jobs based on
// the current distribution of work. If all of the Map jobs have been given out and completed, then
// the coordinator manages sending out Reduce jobs. Once all jobs are completed, the Coordinator
// will have all of the final reduce jobs and the MapReduce job is complete.
func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check if we have any map jobs first that need to be completed
	allMapsDone := true
	for i := range c.MapTasks {
		mapTask := &c.MapTasks[i]
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
			reply.FileName = c.MapTasks[i].FileName
			reply.NReduce = c.NumReduce
			reply.NMap = c.NumMap
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
	for i := range c.ReduceTasks {
		reduceTask := &c.ReduceTasks[i]
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
			reply.NMap = c.NumMap

			// send the file locations to this reduce task to collect intermediate data
			fileLocations := make(map[int]FileLocation)
			for i, t := range c.MapTasks {
				workerData := c.Workers[t.WorkerID]
				fileLocations[i] = FileLocation{
					Address:  workerData.IpAddr,
					Port:     workerData.IpPort,
					WorkerID: t.WorkerID,
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

	c.AllDone = true
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
	taskName := "Map"
	switch args.TaskType {
	// need to check because it can happen that slower tasks finished after a faster completed that beforehand
	case MapTask:
		task = &c.MapTasks[args.TaskID]
	case ReduceTask:
		task = &c.ReduceTasks[args.TaskID]
		taskName = "Reduce"
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

	log.Printf("Completed %s Task %d for ID: %s", taskName, args.TaskID, args.WorkerID)

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

		c.CompletedReduceCount += 1
	}

	// check if this was the last reduce task we were waiting on
	if c.CompletedReduceCount == len(c.ReduceTasks) {
		c.AllDone = true
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
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", c.ServePort))
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
	if c.AllDone {
		// attempt to notify all workers that we are done
		// just send and don't care if we got any errors
		for _, worker := range c.Workers {
			call("WorkerState.TerminateJob", &EmptyArgs{}, &SuccessReply{}, worker.FormatAddress())
		}
	}
	return c.AllDone
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
		Workers:              make(map[string]WorkerData),
		MapTasks:             make([]Task, len(files)),
		ReduceTasks:          make([]Task, nReduce),
		CompletedReduceCount: 0,
		NumMap:               len(files),
		NumReduce:            nReduce,
		AllDone:              false,
		ServePort:            servePort,
	}

	// attempt to load the coordinator state from disk
	if err := c.LoadCoordinatorCheckpoint(); err != nil {
		log.Printf("Failed to load coordinator state: %s", err)
		for i, file := range files {
			c.MapTasks[i] = Task{
				Status:   Idle,
				FileName: file,
			}
		}

		for i := 0; i < nReduce; i++ {
			c.ReduceTasks[i] = Task{
				Status: Idle,
			}
		}
	}

	// spin off a go routine to handle periodic checkpointing of the coordinator
	go func(c *Coordinator) {
		for {
			time.Sleep(SNAPSHOT_FREQUENCY_SECONDS * time.Second)
			if err := c.SaveCoordinatorCheckpoint(); err != nil {
				log.Printf("Failed to save checkpoint: %s", err)
			}
		}
	}(&c)

	// start the RPC server for the coordinator
	c.server()
	return &c
}
