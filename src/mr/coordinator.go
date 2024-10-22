package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Coordinator holds the state for the map-doReduce job
type Coordinator struct {
	pendingFilesToMap    *Queue[string]
	pendingFilesToReduce *Queue[int]
	numFilesToMap        int
	mappedFiles          int
	reducedFiles         int
	nReduce              int
	mu                   sync.Mutex // Mutex for synchronizing access to shared variables
}

// Example RPC handler
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask handles requests from workers for a task
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()         // Acquire the lock
	defer c.mu.Unlock() // Release the lock at the end of the function

	if c.pendingFilesToMap.Size() > 0 {
		// Supplies map tasks
		reply.TaskType = 1
		reply.FileName, _ = c.pendingFilesToMap.Dequeue()
		reply.NReduce = c.nReduce
		fmt.Printf("Supplied map task: %s\n", reply.FileName)
	} else if c.mappedFiles == c.numFilesToMap && c.reducedFiles < c.nReduce {
		// Supplies reduce tasks once all files are mapped
		reply.TaskType = 2
		reply.ReduceJobId, _ = c.pendingFilesToReduce.Dequeue()
		fmt.Printf("Supplied reduce task: %d\n", reply.ReduceJobId)
	} else {
		// Handle the case where no more tasks are available
		reply.TaskType = 0 // Indicate no more tasks
	}

	return nil
}

func (c *Coordinator) NotifyCompletion(args *CompletionArgs, reply *CompletionReply) error {
	c.mu.Lock()         // Acquire the lock
	defer c.mu.Unlock() // Release the lock at the end of the function

	if args.TaskType == 1 {
		c.mappedFiles++
	} else if args.TaskType == 2 {
		c.reducedFiles++
	}
	reply.Ack = 1

	return nil
}

// Start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done checks if the entire job has finished
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.reducedFiles == c.nReduce {
		time.Sleep(10 * time.Second)
		ret = true
	}

	return ret
}

// MakeCoordinator creates a Coordinator and starts the server
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapQueue := NewQueue[string]()
	for _, file := range files {
		mapQueue.Enqueue(file)
	}

	// Initialize the queue for reduce tasks (pendingFilesToReduce)
	reduceQueue := NewQueue[int]()
	for i := 0; i < nReduce; i++ {
		reduceQueue.Enqueue(i)
	}

	c := Coordinator{
		pendingFilesToMap:    mapQueue,
		pendingFilesToReduce: reduceQueue,
		numFilesToMap:        mapQueue.Size(),
		mappedFiles:          0,
		nReduce:              nReduce,
	}

	c.server()
	return &c
}
