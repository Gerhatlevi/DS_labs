package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMap(reply WorkerReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.FileName)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return err
	}

	mapResults := mapf(reply.FileName, string(content))

	// Create nReduce temporary files for consistency
	tmpFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-map-tmp-%d-%d-*", reply.TaskID, i))
		if err != nil {
			return err
		}
		tmpFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
	}

	// Partition KeyValue pairs into buckets same keys get into the same bucket
	for _, kv := range mapResults {
		bucket := ihash(kv.Key) % reply.NReduce
		if err := encoders[bucket].Encode(&kv); err != nil {
			return err
		}
	}

	// Close and atomically rename temporary files to mr-X-Y
	for i := 0; i < reply.NReduce; i++ {
		tmpName := tmpFiles[i].Name()
		if err := tmpFiles[i].Close(); err != nil {
			return err
		}
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		if err := os.Rename(tmpName, finalName); err != nil { // https://www.youtube.com/watch?v=FEIbpHe75vY
			return err
		}
	}

	return nil
}

func handleReduce(reply WorkerReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	// Read all intermediate files mr-0-Y ... mr-(NMap-1)-Y
	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort intermediate key-value pairs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create temporary output file
	tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-reduce-tmp-%d-*", reply.TaskID))
	if err != nil {
		return err
	}

	// Group identical keys and call Reduce on each group
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Required output format: "%v %v\n"
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpName := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		return err
	}

	finalName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	return os.Rename(tmpName, finalName)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := WorkerArgs{}
		reply := WorkerReply{}

		ok := call("Coordinator.GetWork", &args, &reply)
		if !ok {
			// Coordinator has exited, so the job is done
			return
		}

		switch reply.TaskType {
		case MapTask:
			if err := handleMap(reply, mapf); err == nil {
				completeArgs := WorkerArgs{
					TaskType: MapTask,
					TaskID:   reply.TaskID,
				}
				completeReply := WorkerReply{}
				call("Coordinator.CompleteWork", &completeArgs, &completeReply)
			}
		case ReduceTask:
			if err := handleReduce(reply, reducef); err == nil {
				completeArgs := WorkerArgs{
					TaskType: ReduceTask,
					TaskID:   reply.TaskID,
				}
				completeReply := WorkerReply{}
				call("Coordinator.CompleteWork", &completeArgs, &completeReply)
			}
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case ExitTask:
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// If coordinator has exited, dialing fails; return false so worker exits cleanly
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
