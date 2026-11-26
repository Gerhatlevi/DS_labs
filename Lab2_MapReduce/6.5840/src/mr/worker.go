package mr

import (
	"bytes"
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

const DEFAULT_WORKER_RPC_PORT = "5555"

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

func (worker *WorkerState) handleMap(reply WorkerReply, mapf func(string, string) []KeyValue) error {
	content, err := readFile(reply.FileName)
	if err != nil {
		// NOTE: we assume here that the input file is on the local disk to reduce network
		// bandwidth in sending the entire large corpus file at the beginning
		log.Fatal("File is not found on local disk")
	}

	mapResults := mapf(reply.FileName, string(content))

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

func updateKVPairs(reader io.Reader, intermediate []KeyValue) []KeyValue {
	dec := json.NewDecoder(reader)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	return intermediate
}

func (worker *WorkerState) handleReduce(reply WorkerReply, reducef func(string, []string) string) (string, error) {
	intermediate := make(map[int][]KeyValue)

	// keep track of files we need to pull from other workers
	mapSet := make(map[string]FileLocation)

	// Read all intermediate files that are currently on disk mr-0-Y ... mr-(NMap-1)-Y
	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		file, err := os.Open(fileName)

		// we will need to get this file from the other workers
		if err != nil {
			loc := reply.FileLocations[i]
			mapSet[loc.WorkerID] = loc
			continue
		}

		intermediate[i] = updateKVPairs(file, intermediate[i])

		file.Close()
	}

	// get the files we require from the other workers
	for workerId, loc := range mapSet {
		intermediateArgs := IntermediateFileArgs{
			ReduceID: reply.TaskID,
		}
		intermediateReply := IntermediateFileReply{
			BufferedData: make(map[int][]byte),
		}
		if ok := worker.call("WorkerState.GetIntermediateFiles", &intermediateArgs, &intermediateReply, loc.FormatAddress()); ok {
			for mapId, fileBytes := range intermediateReply.BufferedData {
				// update the data from the other worker
				intermediate[mapId] = updateKVPairs(bytes.NewReader(fileBytes), intermediate[mapId])
			}
		} else {
			// if we failed to get the work, we need to tell the coordinator to recompute
			// the work and then we wait and try to get all the data again later
			// hard abort the task
			recomputeArgs := RecomputeWorkArgs{
				WorkerID: workerId,
			}
			recomputeReply := RecomputeWorkReply{}
			log.Panic("What")
			if ok := worker.call("Coordinator.RecomputeWork", &recomputeArgs, &recomputeReply, worker.coordinatorAddress); ok {
				// NOTE: could do something here for if the coordinator reported a failure
				log.Printf("Triggered Recompute Work for ID: %s, %t", workerId, recomputeReply.Success)
			} else {
				// failed to reach the coordinator, ignore this and let the next reduce job report the error
				// to the coordinator when it reaches the same issue
			}
		}
	}

	// coalesce the intermediate keys into a flat structure
	var keyValues []KeyValue
	for _, data := range intermediate {
		keyValues = append(keyValues, data...)
	}

	// Sort intermediate key-value pairs by key
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Key < keyValues[j].Key
	})

	// Create temporary output file
	tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-reduce-tmp-%d-*", reply.TaskID))
	if err != nil {
		return "", err
	}

	// Group identical keys and call Reduce on each group
	i := 0
	length := len(keyValues)
	for i < length {
		j := i + 1

		values := []string{keyValues[i].Value}
		for j < length && keyValues[j].Key == keyValues[i].Key {
			values = append(values, keyValues[j].Value)
			j++
		}

		output := reducef(keyValues[i].Key, values)

		// Required output format: "%v %v\n"
		fmt.Fprintf(tmpFile, "%v %v\n", keyValues[i].Key, output)

		i = j
	}

	tmpName := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		return "", err
	}

	finalName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	return finalName, os.Rename(tmpName, finalName)
}

func initializeWorkerState() *WorkerState {
	ipAddress := os.Getenv("WORKER_IP_ADDR")
	if ipAddress == "" {
		// default to getting the default ip address for the main interface
		ipAddress = getIP()
	} else {
		// validate that this ip address is of valid format
		parsedIP := net.ParseIP(ipAddress)
		// for now we will just force this to be an ipv4 address even though you could
		// use ipv6
		if parsedIP.To4() == nil {
			log.Fatal("WORKER_IP_ADDR is not a valid ipv4 address")
		}
	}
	rpcEnvVar := os.Getenv("WORKER_RPC_PORT")
	if rpcEnvVar == "" {
		rpcEnvVar = DEFAULT_WORKER_RPC_PORT
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

	// actually register this worker with the coordinator to get uuid information
	args := WorkerRegisterArgs{
		IpAddress: ipAddress,
		Port:      servePort,
	}
	reply := WorkerRegisterReply{}
	if ok := worker.call("Coordinator.RegisterWorker", &args, &reply, worker.coordinatorAddress); ok {
		worker.registered = true
		worker.uuid = reply.Uuid
		log.Printf("Registered with UUID: %s", worker.uuid)
	} else {
		log.Fatal("Failed to register worker with coordinator")
		return nil
	}

	return &worker
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := initializeWorkerState()
	// listen for rpc calls from other workers and deliver intermediate files
	worker.server()

	for {
		args := WorkerArgs{}
		reply := WorkerReply{}

		ok := worker.call("Coordinator.GetWork", &args, &reply, worker.coordinatorAddress)
		if !ok {
			// Coordinator has exited, so the job is done
			return
		}

		switch reply.TaskType {
		case MapTask:
			if err := worker.handleMap(reply, mapf); err == nil {
				completeArgs := WorkerArgs{
					TaskType: MapTask,
					TaskID:   reply.TaskID,
					WorkerID: worker.uuid,
				}
				completeReply := WorkerReply{}

				// add the completed map id to our list of local jobs
				worker.mu.Lock()
				worker.mapIDs = append(worker.mapIDs, reply.TaskID)
				worker.mu.Unlock()
				worker.call("Coordinator.CompleteWork", &completeArgs, &completeReply, worker.coordinatorAddress)
			}
		case ReduceTask:
			if filename, err := worker.handleReduce(reply, reducef); err == nil {
				if fileData, err := readFile(filename); err == nil {
					completeArgs := WorkerArgs{
						TaskType:       ReduceTask,
						TaskID:         reply.TaskID,
						WorkerID:       worker.uuid,
						ReduceTaskData: fileData,
					}
					completeReply := WorkerReply{}
					worker.call("Coordinator.CompleteWork", &completeArgs, &completeReply, worker.coordinatorAddress)
				}
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

// GetIntermediateFiles looks up the local intermediate files that were computed as part of the map task ran on
// this worker and reads the file and returns the data.
func (worker *WorkerState) GetIntermediateFiles(args *IntermediateFileArgs, reply *IntermediateFileReply) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	// read all of the files associated with this reduce task
	reply.BufferedData = make(map[int][]byte)
	for _, mapId := range worker.mapIDs {
		if content, err := readFile(fmt.Sprintf("mr-%d-%d", mapId, args.ReduceID)); err == nil {
			reply.BufferedData[mapId] = content
		} else {
			return err
		}
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (worker *WorkerState) call(rpcname string, args interface{}, reply interface{}, addr string) bool {
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	c, err := rpc.DialHTTP("tcp", addr)
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
