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

// **************************
// LATE scheduler parameters
// **************************

// Collected by using a threshold of STRAGGLE_RATE=100
const SlowNodeThreshold = 0.001

// Task should be completed longer than 500 ms
const SlowTaskThreshold = 500

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type WorkerData struct {
	IpAddr              string
	IpPort              int
	WorkerID            string
	AverageProgressRate float64
	MapTaskIDs          map[int]struct{}
	ReduceTaskIDs       map[int]struct{}
}

func (w *WorkerData) FormatAddress() string {
	return fmt.Sprintf("%s:%d", w.IpAddr, w.IpPort)
}

type Task struct {
	Status                 TaskStatus
	FileName               string // only for map tasks
	TaskID                 int
	WorkerID               string
	ProgressScore          float64
	ProgressRate           float64
	TimeToCompletionMillis float64
	StartTime              time.Time
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
	UseLATE              bool
}

const coordinatorCheckpointFile = "Coordinator.checkpoint"

// Helper function to calculate the average progress rate for a particular worker
// and returns that average. To be used in LATE scheduler calculations.
func computeAverageProgressRate(c *Coordinator, worker *WorkerData) float64 {
	averageProgressRate := 0.0
	totalJobs := 0.0
	for mapTaskID := range worker.MapTaskIDs {
		totalJobs += 1
		// only count these jobs if they didn't get co-opted by a speculative job
		if c.MapTasks[mapTaskID].WorkerID == worker.WorkerID {
			averageProgressRate += c.MapTasks[mapTaskID].ProgressRate
		}
	}
	for reduceTaskID := range worker.ReduceTaskIDs {
		totalJobs += 1
		// only count these jobs if they didn't get co-opted by a speculative job
		if c.ReduceTasks[reduceTaskID].WorkerID == worker.WorkerID {
			averageProgressRate += c.ReduceTasks[reduceTaskID].ProgressRate
		}
	}

	if totalJobs > 0 {
		return averageProgressRate / totalJobs
	}
	return 0
}

// SaveCoordinatorCheckpoint is used for periodically writing the Coordinator state c to disk
// which can then be used in LoadCoordinatorCheckpoint for loading from checkpoint. The format
// of the data is just simply writing to json data since the object can be easily marshaled to JSON
func (c *Coordinator) SaveCoordinatorCheckpoint() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//log.Print("Checkpointing the Coordinator State")
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

// Simple function that loads from the checkpointed JSON encoded Coordinator
// state file that was created during SaveCoordinatorCheckpoint.
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

// RegisterWorker is an RPC call that should be the first thing a Worker does to communicate
// with the Coordinator and provide location information (IpAddr and IpPort) so that downstream
// reduce jobs can lookup and fetch intermediate files from each other.
func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid := uuid.New().String()
	c.Workers[uuid] = WorkerData{
		IpAddr:              args.IpAddress,
		IpPort:              args.Port,
		WorkerID:            uuid,
		MapTaskIDs:          make(map[int]struct{}),
		ReduceTaskIDs:       make(map[int]struct{}),
		AverageProgressRate: 0,
	}

	// worker should use this uuid for reporting status information
	reply.WorkerID = uuid
	log.Printf("new uuid: %s, ip: %s, port: %d", uuid, c.Workers[uuid].IpAddr, c.Workers[uuid].IpPort)
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

// UpdateTaskStatus is used for Workers to periodically report their progress score
// back to the coordinator so the coordinator can make a decision about kicking off speculative work.
// If the worker reporting status is not the current one assigned to the job
// or if the task has already been reported complete by another job, then the information is
// just ignored.
func (c *Coordinator) UpdateTaskStatus(args *StatusUpdateArgs, reply *SuccessReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	switch args.TaskType {
	case MapTask:
		if args.TaskID >= 0 && args.TaskID < len(c.MapTasks) {
			task = &c.MapTasks[args.TaskID]
		} else {
			reply.Success = false
			return errors.New("Invalid TaskID provided")
		}
	case ReduceTask:
		if args.TaskID >= 0 && args.TaskID < len(c.ReduceTasks) {
			task = &c.ReduceTasks[args.TaskID]
		} else {
			reply.Success = false
			return errors.New("Invalid TaskID provided")
		}
	default:
		reply.Success = false
		return errors.New("Invalid TaskType reported")
	}

	// only update the statistics for this task if the worker is still assigned to this task
	if task.WorkerID != args.WorkerID {
		reply.Success = false
		return errors.New("Job WorkerID does not match")
	}
	reply.Success = true

	// update the heuristics for this task based on the LATE scheduler:
	// We estimate the progress rate of each task as ProgressScore/T, where T
	// is the amount of time the task has been running for, and then estimate the time to completion as
	// (1 − ProgressScore)/ProgressRate. This assumes that tasks make progress at a roughly constant rate.
	//
	// This information will be used in scheduling speculative jobs in the GetWork function
	task.ProgressScore = args.ProgressScore
	task.ProgressRate = task.ProgressScore / float64(time.Since(task.StartTime).Milliseconds())
	task.TimeToCompletionMillis = (1 - task.ProgressScore) / task.ProgressRate

	// also update the worker average progress rate
	worker := c.Workers[args.WorkerID]
	worker.AverageProgressRate = computeAverageProgressRate(c, &worker)
	c.Workers[args.WorkerID] = worker
	return nil
}

func (c *Coordinator) startTask(task *Task, reply *WorkerReply, workerID string, taskType TaskType) {
	task.Status = InProgress
	task.WorkerID = workerID
	task.StartTime = time.Now()
	// add this task id to the worker MapTasks for lookup and calculations later
	switch taskType {
	case MapTask:
		c.Workers[workerID].MapTaskIDs[task.TaskID] = struct{}{}
	case ReduceTask:
		c.Workers[workerID].ReduceTaskIDs[task.TaskID] = struct{}{}
	}

	// update the reply to the correct data for the worker job
	reply.TaskType = taskType
	reply.TaskID = task.TaskID

	// NOTE: we assume that all workers also have local access to the input
	// corpus files since that is how MapReduce is implemented in the paper
	// where input files reside locally and the coordinator schedules jobs
	// based on that knowledge of where these input files live
	reply.FileName = task.FileName
	reply.NReduce = c.NumReduce
	reply.NMap = c.NumMap
}

// Used for trying to kick off a speculative job. The worker node must be considered a "fast node"
// or rather not a "straggler" which is defined here by if the AverageProgressRate is
// greater than our slow node threshold. Then the task must be in progress and the time
// to completion must meet our criteria for being worth recomputing based on the SlowTaskThreshold.
func (c *Coordinator) shouldStartSpeculative(task *Task, workerID string) bool {
	if c.UseLATE && c.Workers[workerID].AverageProgressRate > SlowNodeThreshold {
		if task.Status == InProgress && task.TimeToCompletionMillis > SlowTaskThreshold {
			return true
		}
	}
	return false
}

// GetWork is used as the primary RPC call for workers to get either Map or Reduce jobs based on
// the current distribution of work. If all of the Map jobs have been given out and completed, then
// the coordinator manages sending out Reduce jobs. Once all jobs are completed, the Coordinator
// will have all of the final reduce jobs and the MapReduce job is complete.
func (c *Coordinator) GetWork(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ********************************************
	// ***************** MAP PHASE ****************
	// ********************************************
	// check if we have any map jobs first that need to be completed
	allMapsDone := true
	longestEstimatedTask := &c.MapTasks[0]
	for i := range c.MapTasks {
		mapTask := &c.MapTasks[i]
		if mapTask.Status != Completed {
			allMapsDone = false
		}

		// used to collect the longest estimated task for speculation later if all tasks are currently running
		if c.UseLATE && mapTask.Status == InProgress && mapTask.TimeToCompletionMillis > longestEstimatedTask.TimeToCompletionMillis {
			longestEstimatedTask = mapTask
		}

		if mapTask.Status == Idle ||
			// backup jobs if it took longer than 10 seconds by default
			(!c.UseLATE && mapTask.Status == InProgress && time.Since(mapTask.StartTime) > 10*time.Second) {
			if mapTask.Status == InProgress {
				log.Printf("Kicking off a backup job for MapTask: %d", mapTask.TaskID)
			}
			c.startTask(mapTask, reply, args.WorkerID, MapTask)
			return nil
		}
	}

	// make the worker wait if we are still working on the map phase
	if !allMapsDone {
		if c.shouldStartSpeculative(longestEstimatedTask, args.WorkerID) {
			log.Printf("Kicking off a speculative job for MapTask: %d", longestEstimatedTask.TaskID)
			c.startTask(longestEstimatedTask, reply, args.WorkerID, MapTask)
		} else {
			reply.TaskType = WaitTask
		}
		return nil
	}

	// ********************************************
	// ************** REDUCE PHASE ****************
	// ********************************************
	// start giving out the reduce jobs since we know the map stage is complete
	allReducesDone := true
	longestEstimatedTask = &c.ReduceTasks[0]
	for i := range c.ReduceTasks {
		reduceTask := &c.ReduceTasks[i]
		if reduceTask.Status != Completed {
			allReducesDone = false
		}

		// used to collect the longest estimated task for speculation later if all tasks are currently running
		if c.UseLATE && reduceTask.Status == InProgress && reduceTask.TimeToCompletionMillis > longestEstimatedTask.TimeToCompletionMillis {
			longestEstimatedTask = reduceTask
		}

		if reduceTask.Status == Idle ||
			(!c.UseLATE && reduceTask.Status == InProgress && time.Since(reduceTask.StartTime) > 10*time.Second) {
			if reduceTask.Status == InProgress {
				log.Printf("Kicking off a backup job for ReduceTask: %d", reduceTask.TaskID)
			}
			c.startTask(reduceTask, reply, args.WorkerID, ReduceTask)

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
		// try to kick off a speculative job. The worker node must be considered a "fast node"
		// or rather not a "straggler" which is defined here by if the AverageProgressRate is
		// greater than our slow node threshold. Then the task must be in progress and the time
		// to completion must meet our criteria for being worth recomputing based on the SlowTaskThreshold.
		if c.shouldStartSpeculative(longestEstimatedTask, args.WorkerID) {
			log.Printf("Kicking off a speculative job for ReduceTask: %d ", longestEstimatedTask.TaskID)
			c.startTask(longestEstimatedTask, reply, args.WorkerID, ReduceTask)
		} else {
			reply.TaskType = WaitTask
		}
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

	log.Printf("Completed %s Task %d for ID: %s\nTask Progress Rate: %f, Worker Progress Rate: %f", taskName, args.TaskID, args.WorkerID, task.ProgressRate, c.Workers[args.WorkerID].AverageProgressRate)

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
	} else {
		// update this worker's average progress rate
		worker := c.Workers[args.WorkerID]
		worker.AverageProgressRate = computeAverageProgressRate(c, &worker)
		c.Workers[args.WorkerID] = worker
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
	rpcEnvVar := os.Getenv("RPC_PORT")
	if rpcEnvVar == "" {
		rpcEnvVar = DEFAULT_COORDINATOR_RPC_PORT
	}
	servePort, err := strconv.Atoi(rpcEnvVar)
	if err != nil {
		log.Fatal("failed to parse environment var RPC_PORT, should be valid port example: RPC_PORT=1234")
	}

	useLATE := false
	if useLATEEnv := os.Getenv("USE_LATE"); useLATEEnv != "" {
		log.Print("Using the LATE scheduler")
		useLATE = true
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
		UseLATE:              useLATE,
	}

	// attempt to load the coordinator state from disk
	if err := c.LoadCoordinatorCheckpoint(); err != nil {
		log.Printf("Failed to load coordinator state: %s", err)
		for i, file := range files {
			c.MapTasks[i] = Task{
				Status:        Idle,
				FileName:      file,
				TaskID:        i,
				ProgressScore: 0,
				ProgressRate:  0,
			}
		}

		for i := 0; i < nReduce; i++ {
			c.ReduceTasks[i] = Task{
				Status:        Idle,
				TaskID:        i,
				ProgressScore: 0,
				ProgressRate:  0,
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
