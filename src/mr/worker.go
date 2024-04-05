package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	alive := true
	for alive {
		task := PullTask()
		switch task.TaskType {
		case TaskType_Map:
			{
				DoMapTask(&task, mapf)
				ReportTaskDone(&task)
			}
		case TaskType_Reduce:
			{
				DoReduceTask(&task, reducef)
				ReportTaskDone(&task)
			}
		case TaskType_Wait:
			{
				// log.Println("All tasks are in progress, waiting...")
				time.Sleep(time.Second)
			}
		case TaskType_Done:
			{
				// log.Println("All tasks are done, Worker exit")
				alive = false
			}
		}
	}
}

func PullTask() Task {
	args := TaskArgs{}
	reply := Task{}
	if ok := call("Coordinator.PushTask", &args, &reply); ok {
		// log.Printf("reply TaskId is %d\n", reply.TaskId)
	} else {
		// log.Printf("call failed!\n")
	}
	return reply
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileNames[0])
	if err != nil {
		log.Fatalf("cannot open %v", task.FileNames)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileNames)
	}
	file.Close()
	reduceNum := task.ReduceNum
	// iterate through each word in the file and return the <word,1> pair
	mapRes := mapf(task.FileNames[0], string(content))
	// hash the word of map result and store it to the hash bucket with corresponding location through the hash value and reduce num
	HashKv := make([][]KeyValue, reduceNum)
	for _, v := range mapRes {
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index], v)
	}
	// put the Map results of reduce num locations in the hash bucket into temp files
	for i := 0; i < reduceNum; i++ {
		filename := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file)
		for _, kv := range HashKv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	reduceNum := task.TaskId
	intermediate := shuffle(task.FileNames)
	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(finalName)
	if err != nil {
		log.Fatal("create file failed:", err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// The keys in the range i and j are the same
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

// Read key-value pairs in all Map intermediate temp file and sort them by key
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, fi := range files {
		file, err := os.Open(fi)
		if err != nil {
			log.Fatalf("cannot open %v", fi)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func ReportTaskDone(task *Task) error {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkTaskDone", &args, &reply)
	if ok {
		// log.Printf("Task [ %d ] done\n", task.TaskId)
	}
	return nil
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

	log.Println(err)
	return false
}
