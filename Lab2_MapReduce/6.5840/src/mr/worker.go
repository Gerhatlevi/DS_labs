package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
	// read the file contents from the file
	file, err := os.Open(reply.fileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.fileName)
	}
	file.Close()

	// if we need to perform a Map
	mapResults := mapf(reply.fileName, string(content))

	// bucket out the results based on ihash % numReduce
	// write these files out to a temporary file
	for _, keyVal := range mapResults {
		reduceID := ihash(keyVal.Key) % reply.nReduce

		// create the output file if not exists
		out, err := os.Create(fmt.Sprintf("mr-%d-%d", reply.workerID, reduceID))
		if err != nil {
			return err
		}

		// write out the key value pair to the intermediate file
		w := bufio.NewWriter(out)
		_, err = w.WriteString(fmt.Sprintf("%s %s", keyVal.Key, keyVal.Value))
		if err != nil {
			return err
		}
		if err = w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func handleReduce(reply WorkerReply, reducef func(string, []string) string) error {
	// need to read in the intermediate file
	// then we simply call the reduce function on that intermediate data
	// content, err := ioutil.ReadAll(file)
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// register with the coordinator and ask for work
	// if no more work is to be done, then we terminate
	// keep asking for work in an infinite loop
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}

		ok := call("Coordinator.GetWork", &args, &reply)
		if ok {
			handleMap(reply, mapf)
			// otherwise we need to perform a Reduce
			handleReduce(reply, reducef)
		}
		return
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
