package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const DEFAULT_RPC_PORT = "5555"

type WorkerState struct {
	mu                 sync.Mutex
	mapIDs             []int
	servePort          int
	coordinatorAddress string
	registered         bool
	uuid               string
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Used for getting the current outbound ip address that can be used for
// taken from https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return string(localAddr.IP)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(fileName string) ([]byte, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return []byte{}, err
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		return []byte{}, err
	}

	return content, err
}

func handleMap(reply WorkerReply, mapf func(string, string) []KeyValue) error {
	mapResults := mapf(reply.FileName, string(reply.FileData))

	// Create nReduce temporary files for consistency
	tmpFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		// we work off of temporary files to reduce the complexity and need for locking each intermediate
		// file in the case of backup jobs or workers writing to the same file.
		// The temporary file is then renamed to the true output file name which is an atomic operation
		// and does not require mutexes.
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
		if err := os.Rename(tmpName, finalName); err != nil {
			// https://www.youtube.com/watch?v=FEIbpHe75vY
			return err
		}
	}

	return nil
}

func handleReduce(reply WorkerReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	// TODO: get the intermediate files from the workers

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

		values := []string{intermediate[i].Value}
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
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

func initializeWorkerState() *WorkerState {
	rpcEnvVar := os.Getenv("WORKER_RPC_PORT")
	if rpcEnvVar == "" {
		rpcEnvVar = DEFAULT_RPC_PORT
	}
	servePort, err := strconv.Atoi(rpcEnvVar)
	if err != nil {
		log.Fatal("failed to parse environment var WORKER_RPC_PORT, should be valid port example: WORKER_RPC_PORT=5555")
	}

	coordinatorAddress := os.Getenv("COORDINATOR_ADDRESS")
	if coordinatorAddress == "" {
		log.Fatal("COORDINATOR_ADDRESS environment variable must be set and include port info like 'localhost:1234'")
	}

	worker := WorkerState{
		mapIDs:             []int{},
		servePort:          servePort,
		coordinatorAddress: coordinatorAddress,
		registered:         false,
	}

	return &worker
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := initializeWorkerState()
	// listen for rpc calls
	worker.server()

	args := WorkerRegisterArgs{
		IpAddress: getIP(),
		Port:      worker.servePort,
	}
	reply := WorkerRegisterReply{}
	if ok := worker.call("Coordinator.RegisterWorker", &args, &reply); ok {
		worker.registered = true
		worker.uuid = reply.Uuid
	} else {
		log.Fatal("Failed to register worker with coordinator")
		return
	}

	for {
		args := WorkerArgs{}
		reply := WorkerReply{}

		ok := worker.call("Coordinator.GetWork", &args, &reply)
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
				worker.call("Coordinator.CompleteWork", &completeArgs, &completeReply)

				// add the completed map id to our list of local jobs
				worker.mapIDs = append(worker.mapIDs, reply.TaskID)
			}
		case ReduceTask:
			if err := handleReduce(reply, reducef); err == nil {
				completeArgs := WorkerArgs{
					TaskType: ReduceTask,
					TaskID:   reply.TaskID,
				}
				completeReply := WorkerReply{}
				worker.call("Coordinator.CompleteWork", &completeArgs, &completeReply)
			}
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case ExitTask:
			return
		}
	}
}

// start a thread that listens for RPCs from other workers and coordinator
func (worker *WorkerState) server() {
	rpc.Register(worker)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", worker.servePort))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (worker *WorkerState) GetIntermediateFiles(args *IntermediateFileRequest, reply *IntermediateFileReply) error {

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (worker *WorkerState) call(rpcname string, args interface{}, reply interface{}) bool {
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	c, err := rpc.DialHTTP("tcp", worker.coordinatorAddress)
	if err != nil {
		// If coordinator has exited, dialing fails; return false so worker exits cleanly
		// TODO: check for if the coordinator is actually down or properly completed
		if !worker.registered {
			log.Fatal("Failed to register worker with coordinator, check COORDINATOR_ADDRESS env var")
		}
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
