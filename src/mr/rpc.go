package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct{}

type TaskType string

const (
	TaskMap    TaskType = "Map"
	TaskReduce TaskType = "Reduce"
	TaskWait   TaskType = "Wait"
	TaskExit   TaskType = "Exit"
)

type TaskReply struct {
	TaskType TaskType
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

type TaskDoneArgs struct {
	TaskType TaskType
	TaskId   int
}

type TaskDoneReply struct{}
