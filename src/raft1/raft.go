package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type serverState string

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // 保护访问此peer的共享访问（包括投票和当前任期）
	peers            []*labrpc.ClientEnd // 所有Raft服务器的RPC端点
	persister        *tester.Persister   // 持久化状态的对象，用来保存Raft状态，以便在崩溃和重启后恢复
	me               int                 // 当前节点在peers[]中的索引
	state            serverState         // 当前节点的状态（Follower、Candidate或Leader）
	currentTerm      int                 // 当前任期号
	log              []any               // 日志条目，包含命令和任期号
	VoteFor          int
	heartbeat        *time.Timer //心跳超时
	overElectiontime *time.Timer //选举超时
}

// return currentTerm and whether this server
// believes it is the leader.
// 获取当前节点任期与是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

type HeartbeatArgs struct {
	LeaderId   int
	Leaderterm int
}

type HeartbeatReply struct {
	Success bool
	Term    int
}

// 发送心跳
func (rf *Raft) AppendEntries(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Leaderterm < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	rf.state = Follower
	if args.Leaderterm > rf.currentTerm {
		rf.currentTerm = args.Leaderterm
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) SendAppendEntries(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 持久化保存当前raft状态，防止节点崩溃
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
// 重新更新持久化信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
// 读取raft日志中多少bytes
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 发起选举的结构体
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // 候选人当前的任期号
	CandidateId int // 请求选票的候选人ID（服务器的索引）
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票的结构体
type RequestVoteReply struct {
	IsVote int
	Term   int
}

// example RequestVote RPC handler.
// 接收投票请求，投出票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.IsVote = 0
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.VoteFor = -1
		rf.state = Follower
	}

	if args.Term == rf.currentTerm &&
		(rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {

		rf.VoteFor = args.CandidateId
		reply.IsVote = 1
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 发送投票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 执行客户端发来的指令（是leader的话，不是直接返回-1让客户端继续找别的服务器）
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// 无限循环接受心跳，心跳失败发送选举
func (rf *Raft) ticker() {
	for {
		switch rf.state {

		case Candidate: //选举者发送选举

			time.Sleep(50 * time.Millisecond)
			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				continue
			}

			rf.currentTerm++
			rf.VoteFor = rf.me
			rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			votes := 1
			rf.mu.Unlock()
			//finished := 1
			for n := range rf.peers {
				if n != rf.me {
					go func(i int) {
						reply := &RequestVoteReply{}
						ok := rf.sendRequestVote(i, args, reply)

						rf.mu.Lock()
						defer rf.mu.Unlock()

						if !ok {
							return
						}

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.VoteFor = -1
							return
						}

						if rf.state != Candidate {
							return
						}

						if reply.IsVote == 1 && reply.Term == rf.currentTerm {
							votes++
							if votes > len(rf.peers)/2 && rf.state == Candidate {
								rf.state = Leader
							}
						}
					}(n)
				}
			}
		case Follower:
			select {
			case <-rf.overElectiontime.C:

				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
			default:
			}
		case Leader:

			rf.mu.Lock()
			args := &HeartbeatArgs{
				LeaderId:   rf.me,
				Leaderterm: rf.currentTerm,
			}
			rf.mu.Unlock()

			for n := range rf.peers {
				if n != rf.me {
					go func(i int) {
						reply := &HeartbeatReply{}
						rf.SendAppendEntries(i, args, reply) //发送心跳

						if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.VoteFor = -1
							rf.mu.Unlock()
						}
					}(n)

				}
			}
		}
		//ms := 50 + (rand.Int63() % 300)
		time.Sleep(50 * time.Millisecond)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.VoteFor = -1
	rf.overElectiontime = time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	rf.heartbeat = time.NewTimer(50 * time.Millisecond)
	//rf.heartbeat
	//rf.log
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
