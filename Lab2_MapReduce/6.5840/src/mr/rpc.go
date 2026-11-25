package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Design communicatin

type TaskType int

const (
	MapTask TaskType = iota // enum in go lol who came up with it
	ReduceTask
	WaitTask // Wait for all map tasks to be finished
	ExitTask
)

type WorkerRegisterArgs struct {
	IpAddress string
	Port      int
}

type WorkerRegisterReply struct {
	Uuid string
}

// Worker send this to coordinator, if Worker is looking for a task this is empty, after completing values will be set
type WorkerArgs struct {
	TaskType TaskType
	TaskID   int
	WorkerID string
}

// Coordinator answer to Worker (is used with reference instead of return value)
// taskname: mr-X-Y :::: X is the map Y is the reduce value
type WorkerReply struct {
	TaskType TaskType
	TaskID   int
	FileName string // only for map tasks
	FileData []byte // only for map tasks
	NReduce  int    // used in map to know the number of buckets
	NMap     int    // used in reduce to know the number of output files from map
}

// Used for workers to communicate with each other to get all intermediate files for a job
// This request is only used in the reduce stage for workers to collect the
// intermediate files from all of the other workers for a particular bucket.
type IntermediateFileRequest struct {
	ReduceID int
}

// Used to reply with the actual file data requested by the reduce worker task
type IntermediateFileReply struct {
	BufferedData [][]byte
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
