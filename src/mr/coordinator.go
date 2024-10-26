package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerId int
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// GetReduceCount RPC handler.
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)

	return nil
}

// RequestTask RPC handler.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	var task *Task
	if c.nMap > 0 {
		task = c.selectTask(c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
		task = c.selectTask(c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{ExitTask, Finished, -1, "", -1}
	}

	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.TaskFile = task.File

	c.mu.Unlock()
	go c.waitForTask(task)

	return nil
}

// ReportTaskDone RPC handler.
func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	/* Ensure the same worker did the same task. In case worker1 froze and task got reassigned to worker2. And while
	worker2 was working and worker1 completed, we need to omit this result because we already reassigned it.

	Basically the task can be marked completely only if the recently executing worker completes it. This prevents
	c.nMap and c.nReduce from getting incorrect values.
	*/
	if args.WorkerId == task.WorkerId && task.Status == Executing {
		task.Status = Finished
		if args.TaskType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.TaskType == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	// to exit last worker
	reply.CanExit = c.nMap == 0 && c.nReduce == 0

	return nil
}

func (c *Coordinator) selectTask(taskList []Task, workerId int) *Task {
	var task *Task

	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerId = workerId
			return task
		}
	}

	return &Task{NoTask, Finished, -1, "", -1}
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(time.Second * TaskTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Status == Executing {
		task.Status = NotStarted
		task.WorkerId = -1
	}
}

// start a thread that listens for RPCs from worker.go
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

// Done is called periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nMap == 0 && c.nReduce == 0
}

// MakeCoordinator creates a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, nMap)
	c.reduceTasks = make([]Task, 0, nReduce)

	for i := 0; i < nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		c.mapTasks = append(c.mapTasks, mTask)
	}
	for i := 0; i < nReduce; i++ {
		rTask := Task{ReduceTask, NotStarted, i, "", -1}
		c.reduceTasks = append(c.reduceTasks, rTask)
	}

	c.server()

	// remove all previous intermediate solutions
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}

	// remove temp directory
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &c
}
