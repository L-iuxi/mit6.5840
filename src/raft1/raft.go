package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type LogInf struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // 保护访问此peer的共享访问（包括投票和当前任期）
	peers            []*labrpc.ClientEnd // 所有Raft服务器的RPC端点
	persister        *tester.Persister   // 持久化状态的对象，用来保存Raft状态，以便在崩溃和重启后恢复
	me               int                 // 当前节点在peers[]中的索引
	state            serverState         // 当前节点的状态（Follower、Candidate或Leader）
	currentTerm      int                 // 当前任期号
	log              []LogInf            // 日志条目，包含命令和任期号
	VoteFor          int
	heartbeat        *time.Timer //心跳超时
	overElectiontime *time.Timer //选举超时

	nextIndex   []int //日志同步的位置（从哪里开始同步日志
	commitIndex int   //提交成功的日志
	matchIndex  []int

	lastApplied int //上次commit
}

// 获取当前节点任期与是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

type HeartbeatArgs struct {
	LeaderId          int
	LeaderTerm        int
	Entries           []LogInf
	PreLogIndex       int //最后对齐位置
	PreLogTerm        int //最后对齐位置的任期
	LeaderCommitIndex int
}

type HeartbeatReply struct {
	Success       bool
	Term          int
	ConflictIndex int //冲突位置
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			next := rf.nextIndex[peer]
			prevIndex := next - 1

			var prevTerm int

			if prevIndex >= 0 {
				prevTerm = rf.log[prevIndex].Term
			}

			entries := make([]LogInf, len(rf.log[next:]))
			copy(entries, rf.log[next:])

			args := &HeartbeatArgs{
				LeaderId:          rf.me,
				LeaderTerm:        rf.currentTerm,
				Entries:           entries,
				PreLogIndex:       prevIndex,
				PreLogTerm:        prevTerm,
				LeaderCommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &HeartbeatReply{}
			ok := rf.SendAppendEntries(peer, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader || args.LeaderTerm != rf.currentTerm {
				return
			}

			// if !reply.Success {
			// 	rf.nextIndex[peer] = reply.ConflictIndex
			// }
			if reply.Success {
				if len(args.Entries) > 0 {

					rf.nextIndex[peer] = args.PreLogIndex + len(args.Entries) + 1
					//成功对齐日志，记录成功数
					rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
				} else {
					rf.nextIndex[peer] = args.PreLogIndex + 1
				}
				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
					count := 1
					for j := range rf.peers {
						if j != rf.me && rf.matchIndex[j] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
						rf.commitIndex = N
						break
					}
				}
			} else {
				// rf.nextIndex[peer] = max(0, rf.nextIndex[peer]-1)
				rf.nextIndex[peer] = reply.ConflictIndex
				if rf.nextIndex[peer] > len(rf.log) {
					rf.nextIndex[peer] = len(rf.log)
				}

				// if rf.nextIndex[peer] < 1 {
				// 	rf.nextIndex[peer] = 1
				// }
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.VoteFor = -1
				rf.persist()
			}
		}(i)
	}
}

// 发送心跳
func (rf *Raft) AppendEntries(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm < rf.currentTerm { //leader任期落后
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		return
	}

	rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	if args.LeaderTerm > rf.currentTerm { //自己落后，更新任期
		rf.currentTerm = args.LeaderTerm
		rf.VoteFor = -1
		rf.persist()
	}
	rf.state = Follower

	if args.PreLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		return
	}

	if args.PreLogIndex >= 0 && rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		index := args.PreLogIndex

		conflictTerm := rf.log[args.PreLogIndex].Term
		for index >= 0 && rf.log[index].Term == conflictTerm {
			index--
		}
		reply.ConflictIndex = index + 1
		return
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.log)-1)
	}

	// if args.PreLogIndex <= 0 {
	// 	// leader 还没日志，直接覆盖
	// 	rf.log = append([]LogInf{{Term: 0}}, args.Entries...)
	// 	rf.persist()
	// } else {
	//查重
	index := args.PreLogIndex + 1

	for i, entry := range args.Entries {
		if index+i < len(rf.log) {
			if rf.log[index+i].Term != entry.Term { //发生冲突

				rf.log = rf.log[:index+i] //从当前开始覆盖后面所有
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}

		// }

		// rf.log = rf.log[:args.PreLogIndex+1]
		// rf.log = append(rf.log, args.Entries...) //同步日志
	}

	if len(args.Entries) > 0 {
		newLen := args.PreLogIndex + len(args.Entries) + 1
		if newLen < len(rf.log) {
			rf.log = rf.log[:newLen]
		}
	}

	rf.persist()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.VoteFor)
	e.Encode(rf.currentTerm)

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)

}

// 解码持久化信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Log []LogInf
	var Term int
	var Vote int

	if d.Decode(&Log) != nil || d.Decode(&Vote) != nil || d.Decode(&Term) != nil {
		//解码失败
	} else {
		rf.log = Log
		rf.VoteFor = Vote
		rf.currentTerm = Term
	}

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

	LastLogIndex int //最新日志index
	LastLogTerm  int //最新日志term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票的结构体
type RequestVoteReply struct {
	IsVote int
	Term   int
}

// 接收投票请求，投出票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.IsVote = 0

	upToDate := false
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		upToDate = true
	}

	if args.Term > rf.currentTerm { //版本号大了，更新版本号，清空投票
		rf.currentTerm = args.Term
		rf.VoteFor = -1
		rf.state = Follower
		rf.persist()
	}

	if args.Term < rf.currentTerm { //版本号小了，不投票
		reply.IsVote = 0
		reply.Term = rf.currentTerm
		return
	}

	if args.Term == rf.currentTerm &&
		(rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && upToDate { //版本号相同，未投票

		rf.VoteFor = args.CandidateId
		reply.IsVote = 1
		rf.persist()
		rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
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
// 复制日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	index := len(rf.log) //日志编号
	isleader := true

	if rf.state != Leader {
		isleader = false
		return -1, term, isleader
	} //不是leader不复制

	newcomm := LogInf{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newcomm)
	rf.persist()
	go rf.broadcastAppendEntries()

	return index, term, isleader
}

// 无限循环接受心跳，心跳失败发送选举
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {

		case Follower, Candidate:
			select {
			case <-rf.overElectiontime.C: //触发超时选举

				rf.mu.Lock() //锁

				rf.state = Candidate
				rf.currentTerm++
				rf.VoteFor = rf.me

				term := rf.currentTerm
				me := rf.me

				rf.persist()
				// 重置选举超时
				if !rf.overElectiontime.Stop() {
					select {
					case <-rf.overElectiontime.C:
					default:
					}
				}
				rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

				votes := 1
				lastIndex := len(rf.log) - 1
				lastTerm := rf.log[lastIndex].Term
				rf.mu.Unlock()

				for i := range rf.peers {
					if i == me {
						continue
					}

					go func(server int) {

						args := &RequestVoteArgs{
							Term:         term,
							CandidateId:  me,
							LastLogIndex: lastIndex,
							LastLogTerm:  lastTerm,
						}
						reply := &RequestVoteReply{}

						ok := rf.sendRequestVote(server, args, reply)
						if !ok {
							return
						}

						rf.mu.Lock()

						// 过滤旧term
						if rf.state != Candidate || rf.currentTerm != term {
							rf.mu.Unlock()
							return
						}

						//发现更高term
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.VoteFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						if reply.IsVote == 1 {

							rf.mu.Lock()
							votes++

							if votes > len(rf.peers)/2 && rf.state == Candidate {

								rf.state = Leader
								rf.heartbeat.Reset(0)

								// 当选为leader之后立刻发一次心跳告诉所有人

								for j := range rf.peers {
									rf.nextIndex[j] = len(rf.log) //立刻更新对齐数
								}
								// 	if j != rf.me {
								// 		go func(peer int) {

								// 			rf.mu.Lock()
								// 			defer rf.mu.Unlock()
								// 			next := rf.nextIndex[peer]
								// 			prevIndex := next - 1

								// 			var prevTerm int

								// 			if prevIndex >= 0 {
								// 				prevTerm = rf.log[prevIndex].Term
								// 			}

								// 			entries := make([]LogInf, len(rf.log[rf.nextIndex[peer]:]))
								// 			copy(entries, rf.log[rf.nextIndex[peer]:])

								// 			args := &HeartbeatArgs{
								// 				LeaderId:          rf.me,
								// 				LeaderTerm:        rf.currentTerm,
								// 				Entries:           entries, //只发送未同步的日志
								// 				PreLogIndex:       prevIndex,
								// 				PreLogTerm:        prevTerm,
								// 				LeaderCommitIndex: rf.commitIndex,
								// 			}
								// 			reply := &HeartbeatReply{}
								// 			rf.SendAppendEntries(peer, args, reply)

								// 		}(j)
								// 	}
								// }
							}
							rf.mu.Unlock()
						}
					}(i)
				}
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader: //leader发送心跳

			select {
			case <-rf.heartbeat.C:
				rf.broadcastAppendEntries() //心跳
				// // count := 1
				// for n := range rf.peers {
				// 	if n != rf.me { //给除自己以外的所有发送心跳
				// 		go func(i int) {

				// 			rf.mu.Lock()
				// 			next := rf.nextIndex[i]
				// 			prevIndex := next - 1

				// 			var prevTerm int

				// 			if prevIndex >= 0 {
				// 				prevTerm = rf.log[prevIndex].Term
				// 			}

				// 			entries := make([]LogInf, len(rf.log[rf.nextIndex[i]:]))
				// 			copy(entries, rf.log[rf.nextIndex[i]:])

				// 			args := &HeartbeatArgs{
				// 				LeaderId:          rf.me,
				// 				LeaderTerm:        rf.currentTerm,
				// 				Entries:           entries, //只发送未同步的日志
				// 				PreLogIndex:       prevIndex,
				// 				PreLogTerm:        prevTerm,
				// 				LeaderCommitIndex: rf.commitIndex,
				// 			}
				// 			rf.mu.Unlock()

				// 			reply := &HeartbeatReply{}
				// 			ok := rf.SendAppendEntries(i, args, reply)
				// 			if !ok {
				// 				return
				// 			}

				// 			rf.mu.Lock()
				// 			defer rf.mu.Unlock()

				// 			if rf.state != Leader || args.LeaderTerm != rf.currentTerm {
				// 				return
				// 			}

				// 			if reply.Success {
				// 				if len(args.Entries) > 0 {

				// 					rf.nextIndex[i] = args.PreLogIndex + len(args.Entries) + 1
				// 					//成功对齐日志，记录成功数
				// 					rf.matchIndex[i] = args.PreLogIndex + len(args.Entries)
				// 				} else {
				// 					rf.nextIndex[i] = args.PreLogIndex + 1
				// 				}
				// 				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {

				// 					count := 1 // leader 自己

				// 					for j := range rf.peers {
				// 						if j != rf.me && rf.matchIndex[j] >= N {
				// 							count++
				// 						}
				// 					}

				// 					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
				// 						rf.commitIndex = N
				// 						break
				// 					}
				// 				}
				// 			} else {
				// 				rf.nextIndex[i] = max(0, rf.nextIndex[i]-1) //找最后一个一样的节点
				// 			}

				// 			if reply.Term > rf.currentTerm { //自己的任期号小了，别人是leader
				// 				rf.currentTerm = reply.Term
				// 				rf.state = Follower
				// 				rf.VoteFor = -1
				// 				rf.persist()
				// 			}

				// 		}(n)
				// 	}
				// }
				rf.heartbeat.Reset(50 * time.Millisecond)
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
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
// 初始化raft结构体
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.VoteFor = -1
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.log = []LogInf{{Term: 0}}
	rf.overElectiontime = time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	rf.heartbeat = time.NewTimer(50 * time.Millisecond)
	//rf.heartbeat
	//rf.log
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//按照commitindex commit command
	go func() {
		for {
			var msgs []raftapi.ApplyMsg
			rf.mu.Lock()
			for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < len(rf.log) {
				rf.lastApplied++
				msgs = append(msgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				})
			}
			rf.mu.Unlock()
			for _, msg := range msgs {
				applyCh <- msg
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	//开始检查心跳，发起选举
	go rf.ticker()

	return rf
}
