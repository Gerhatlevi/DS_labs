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

// Worker send this to coordinator, if Worker is looking for a task this is empty, after completing values will be set
type WorkerArgs struct {
	TaskType TaskType
	TaskID   int
}

// Coordinator answer to Worker (is used with reference instead of return value)
// taskname: mr-X-Y :::: X is the map Y is the reduce value
type WorkerReply struct {
	TaskType TaskType
	TaskID   int
	FileName string // only for map tasks
	NReduce  int    // used in map to know the number of buckets
	NMap     int    // used in reduce to know the number of output files from map
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
