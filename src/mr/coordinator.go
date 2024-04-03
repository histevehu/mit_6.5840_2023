package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	State      int        // Map Reduce phase
	MapChan    chan *Task // Map task channel
	ReduceChan chan *Task // Reduce task channel
	ReduceNum  int        // Reduce amount
	Files      []string   // files
}

type Task struct {
	TaskType  int    // task status：Map, Reduce
	FileName  string // file
	TaskId    int    // task ID, used for temp files generation
	ReduceNum int    // Reduce amount
}

const (
	TaskType_Map=iota
	TaskType_Reduce
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) PushTask(args *TaskArgs, reply *Task) error {
	*reply = *<-c.MapChan
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{State: 0,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: make(chan *Task, nReduce),
		ReduceNum:  nReduce,
		Files:      files}
	c.MakeMapTasks(files)

	c.server()
	return &c
}

func (c *Coordinator) MakeMapTasks(files []string) {
	// Create a map reduce task for each file
	for id, v := range files {
		task := Task{TaskType: TaskType_Map,
			FileName:  v,
			TaskId:    id,
			ReduceNum: c.ReduceNum}
		c.MapChan <- &task

		fmt.Println("Made map task: ",v)
	}
}
