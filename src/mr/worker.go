package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
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
type ByKey []KeyValue

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
func doMap(reply TaskReply, mapf func(string, string) []KeyValue) {
	content, _ := os.ReadFile(reply.FileName)
	kva := mapf(reply.FileName, string(content))
	files := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		f, _ := os.CreateTemp("", "mr-map-*")
		files[i] = f
		encoders[i] = json.NewEncoder(f)
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		encoders[idx].Encode(kv)
	}
	for _, f := range files {
		f.Close()
	}
	for i := 0; i < reply.NReduce; i++ {
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		os.Rename(files[i].Name(), finalName)
	}
}
func doReduce(reply TaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}cd ../main
go run mrcoordinator.go pg-*.txt

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	ofile.Close()
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := TaskArgs{}
		reply := TaskReply{}

		ok := call("Coordinator.AssignTask", &args, &reply)

		if !ok {
			fmt.Printf("call failed!\n")
			return
		}

		switch reply.TaskType {
		case "Map":
			doMap(reply, mapf)

			doneArgs := TaskDoneArgs{
				TaskType: "Map",
				TaskId:   reply.TaskId,
			}
			call("Coordinator.TaskDone", &doneArgs, &TaskDoneReply{})
		case "Reduce":
			doReduce(reply, reducef)

			doneArgs := TaskDoneArgs{
				TaskType: "Reduce",
				TaskId:   reply.TaskId,
			}
			call("Coordinator.TaskDone", &doneArgs, &TaskDoneReply{})
		case "Wait":
			time.Sleep(time.Second)
		case "Exit":
			return

		}
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
