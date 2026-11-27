package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
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
	WorkerID string
}

type StatusUpdateArgs struct {
	WorkerID      string
	TaskType      TaskType
	TaskID        int
	ProgressScore float64
}

type FileLocation struct {
	WorkerID string
	Address  string
	Port     int
}

// FormatAddress is a string format function to make it easy for
// consumers of FileLocation to get the proper string formatted address
func (loc *FileLocation) FormatAddress() string {
	return fmt.Sprintf("%s:%d", loc.Address, loc.Port)
}

// Worker send this to coordinator, if Worker is looking for a task this is empty,
// after completing values will be set
type WorkerArgs struct {
	TaskType       TaskType
	TaskID         int
	WorkerID       string
	ReduceTaskData []byte // used for sending the final reduce file data back to the coordinator
}

// Coordinator answer to Worker (is used with reference instead of return value)
// taskname: mr-X-Y :::: X is the map Y is the reduce value
type WorkerReply struct {
	TaskType      TaskType
	TaskID        int
	FileName      string               // only for map tasks
	NReduce       int                  // used in map to know the number of buckets
	NMap          int                  // used in reduce to know the number of output files from map
	FileLocations map[int]FileLocation // used in reduce to lookup intermediate data locations
}

// Used for workers to communicate with each other to get all intermediate files for a job
// This request is only used in the reduce stage for workers to collect the
// intermediate files from all of the other workers for a particular bucket.
type IntermediateFileArgs struct {
	ReduceID int
}

// Used to reply with the actual file data requested by the reduce worker task
// The BufferedData map is a mapping of map task id to the intermediate data for
// that job. This is used for the reduce worker to detect if it has all the data
// it needs to continue working or not.
type IntermediateFileReply struct {
	BufferedData map[int][]byte
}

type RecomputeWorkArgs struct {
	WorkerID string
}

type EmptyArgs struct {
}

type SuccessReply struct {
	Success bool
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
