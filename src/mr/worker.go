package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	// TODO: Your worker implementation here.
	for {
		args := TaskArgs{}
		reply := TaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)

		if ok {
			switch reply.TaskType {
			case 0:
				return

				// Map task
			case 1:
				doMap(mapf, reply.FileName, reply.NReduce)

				completionArgs := CompletionArgs{FileName: reply.FileName, TaskType: reply.TaskType}
				completionReply := CompletionReply{}
				call("Coordinator.NotifyCompletion", &completionArgs, &completionReply)

				// Reduce task
			case 2:
				doReduce(reducef, reply.ReduceJobId)

				completionArgs := CompletionArgs{ReduceJobId: reply.ReduceJobId, TaskType: reply.TaskType}
				completionReply := CompletionReply{}
				call("Coordinator.NotifyCompletion", &completionArgs, &completionReply)

			}
		} else {
			fmt.Printf("call failed!\n")
			time.Sleep(10 * time.Second)
		}
	}

}

func doMap(mapf func(string, string) []KeyValue, filename string, nReduce int) {
	fmt.Printf("Start to process file %s\n", filename)
	// Open and read the file content.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Apply the map function.
	kva := mapf(filename, string(content))

	// Create a directory named "intermediate_files" in the current working directory.
	intermediateDir := "intermediate_files"
	err = os.MkdirAll(intermediateDir, os.ModePerm)
	if err != nil {
		log.Fatalf("cannot create directory: %v", intermediateDir)
	}

	// Create pendingFilesToMap for intermediate output based on ihash % nReduce.
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	// Create pendingFilesToMap and encoders.
	for i := 0; i < nReduce; i++ {
		intermediateFilename := filepath.Join(intermediateDir, fmt.Sprintf("mr-%s-%d", filepath.Base(filename), i))
		intermediateFiles[i], err = os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFilename)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}

	// Distribute key-value pairs across the pendingFilesToMap.
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	// Close all intermediate pendingFilesToMap.
	for _, file := range intermediateFiles {
		file.Close()
	}
}

func doReduce(reducef func(string, []string) string, id int) {
	intermediate := []KeyValue{}

	// Compile the regex to match filenames with the pattern "mr-<any string>-<id>"
	pattern := fmt.Sprintf("mr-.*-%d", id)
	re := regexp.MustCompile(pattern)

	// Iterate over all files in the "intermediate_files" directory
	files, err := ioutil.ReadDir("intermediate_files")
	if err != nil {
		log.Fatalf("cannot read intermediate_files directory: %v", err)
	}

	for _, file := range files {
		if re.MatchString(file.Name()) {
			// Open the file and decode its key-value pairs
			filePath := filepath.Join("intermediate_files", file.Name())
			f, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			f.Close()
		}
	}

	// Sort the intermediate key-value pairs by key
	sort.Sort(ByKey(intermediate))

	// Prepare the output file
	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create output file %v: %v", oname, err)
	}

	// Perform the reduce operation on each unique key
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Write the reduce output to the file
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// Close the output file
	ofile.Close()
}

// readIntermediateFile reads the key-value pairs from the specified intermediate file.
func readIntermediateFile(filename string) []KeyValue {
	var kva []KeyValue

	// Open the file.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	// Decode the JSON objects from the file.
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva
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
