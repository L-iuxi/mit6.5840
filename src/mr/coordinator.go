package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskStatus int

const(
	Idle = iota
	Running
	Done
)

type Task struct{
	TaskId int
	FileName string
	Status TaskStatus
	StartTime time.Time
}

type Phase int

const(
	MapPhase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	
	mapTasks []Task
	reduceTasks []Task
	phase Phase
	mu sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs,reply *TaskReply)error{

	c.mu.Lock()
    defer c.mu.Unlock()

	if c.phase == AllDone{
		reply.TaskType = "Exit"
		return nil
	}
	if c.phase == MapPhase{
		for i := range c.mapTasks{
			task := &c.mapTasks[i]

			if task.Status == Idle{
				task.Status = Running
				task.StartTime = time.Now()

				reply.TaskType = "Map"
				reply.TaskId = task.TaskId
				reply.FileName = task.FileName
				reply.NReduce = len(c.reduceTasks)
				return nil
			}
		}
		reply.TaskType = "Wait"
		return nil	
	}
	if c.phase == ReducePhase{
		for i := range c.reduceTasks{
			task := &c.reduceTasks[i]

			if task.Status == Idle{
				task.Status = Running
				task.StartTime = time.Now()

				reply.TaskType = "Reduce"
				reply.TaskId = task.TaskId
				return nil
			}
		}
		reply.TaskType = "Wait"
		return nil
	}
	return nil	
}
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}
func allTasksDone(tasks []Task) bool {
	for _, t := range tasks {
		if t.Status != Done{
		return false
			}
		}
	return true
}
func (c *Coordinator) TaskDone(args *TaskDoneArgs,reply *TaskDoneReply)error
{
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapPhase{
		if args.TaskId >= 0 && args.TaskId < len(c.mapTasks){
			c.mapTasks[args.TaskId].Status = Done
		}
	if allTasksDone(c.mapTasks){
		c.Phase = ReducePhase
	}
	}else if c.Phase == ReducePhase{
		if args.TaskId >= 0 && args.TaskId < len(c.reduceTasks){
			c.reduceTasks[args.TaskId].Status = Done
		}
	if allTasksDone(c.reduceTasks){
		c.Phase = AllDone
		}
	}
	return nil
}
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	if c.phase == AllDone{
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = MapPhase
	for i ,file := range files{
		task := Task{
			TaskId : i
    		FileName : file
			Status : Idle
		}

		c.mapTasks = c.MapTask.append(task)
	}

	c.server(sockname)
	return &c
}
