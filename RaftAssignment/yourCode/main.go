package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node. invode new raft node
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever. stuck here, not consume CPU
	select {}
}

type raftNode struct {
	log []*raft.LogEntry
	// TODO: Implement this!
	mu sync.Mutex

	//id            int
	//nodeidPortMap map[int]int
	kvstore map[string]string

	server *raft.RaftNodeServer

	//peersIds []int

	//persistent state
	currentTerm int32
	votedFor    int32

	//Volatile state on all servers
	serverState raft.Role
	commitIndex int32
	commitChan  chan bool

	electionTimeout   int32
	heartBeatInterval int32

	resetChan chan bool //use one channel to reset a timer

	heartBeatChan   chan bool
	followerChan    chan bool
	leaderChan      chan bool
	grantedVoteChan chan bool

	voteCount int

	nextIndex  []int32
	matchIndex []int32

	commitNum int32
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	//initialize node here!
	rn := raftNode{
		log: make([]*raft.LogEntry, 0),
		//log: nil,

		//id:            nodeId,
		//nodeidPortMap: nodeidPortMap,

		serverState: raft.Role_Follower,

		heartBeatInterval: int32(heartBeatInterval),
		electionTimeout:   int32(electionTimeout),
		resetChan:         make(chan bool, 1), //make a channel,bufferSize 1

		heartBeatChan:   make(chan bool),
		followerChan:    make(chan bool),
		leaderChan:      make(chan bool),
		grantedVoteChan: make(chan bool),

		//finishState: false,

		votedFor:    -1,
		currentTerm: 0,
		voteCount:   0,

		nextIndex:  make([]int32, len(hostConnectionMap)),
		matchIndex: make([]int32, len(hostConnectionMap)),

		commitIndex: 0,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes, build connection, communicate
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//context
	ctx := context.Background()
	//lock
	rn.log = append(rn.log, &raft.LogEntry{})

	//TODO: kick off leader election here !
	go func() {
		for {
			switch rn.serverState {

			case raft.Role_Follower:
				//TODO
				select {
				//Timer for election, follower wait until the election Timeout   heartbeat interval used by leader to send heartbeat
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
					rn.serverState = raft.Role_Candidate //election time out, be the candidate, ask other node to vote for him
				case <-rn.resetChan:
					//do nothing, still be the follower, use another channel to reset timer
				case <-rn.heartBeatChan:
					//do nothing when receiving heartBeat
				case <-rn.grantedVoteChan:
				}
			case raft.Role_Candidate:

				//TODO
				//follower become candidate, get majority vote to be the leader, ask vote from other Raft node
				//Term: default = 0, become candidate, term += 1
				rn.mu.Lock()

				rn.currentTerm += 1
				//vote for itself
				rn.votedFor = int32(nodeId)
				//invoke RequestVote() function of other node using RPC
				//voteNum := 0 //0 or 1
				rn.voteCount = 1

				rn.mu.Unlock()

				//how to get the client: hostConnectionMap   ASK FOR VOTE
				for hostId, client := range hostConnectionMap {
					//for each raftNode invoke a go routine to request vote, no wait
					/*
						if hostId == int32(nodeId) {
							continue
						}*/

					go func() {

						rn.mu.Lock()
						defer rn.mu.Unlock()
						//return value, error

						r, err := client.RequestVote(
							//raft.RequestVoteArgs
							ctx,
							&raft.RequestVoteArgs{
								From:         int32(nodeId),
								To:           int32(hostId),
								Term:         rn.currentTerm,
								CandidateId:  int32(nodeId),
								LastLogIndex: int32(len(rn.log) - 1),
								LastLogTerm:  int32(rn.log[len(rn.log)-1].GetTerm()),
							},
						)

						if err == nil && r.Term > rn.currentTerm {
							rn.serverState = raft.Role_Follower
							rn.currentTerm = r.Term
							rn.votedFor = -1
						}

						if err == nil && r.VoteGranted == true && r.Term == rn.currentTerm {
							//TODO:add a lock here
							rn.mu.Lock()
							//voteNum++ //may be concurrently write
							rn.voteCount++
							//TODO:release the lock
							rn.mu.Unlock()
							if rn.voteCount == len(hostConnectionMap)/2 && rn.serverState == raft.Role_Candidate { //majority of vote,
								rn.votedFor = -1
								rn.serverState = raft.Role_Leader //state should be only changed once, serverState not necessary to change again

								rn.leaderChan <- true
							}

							if rn.voteCount < len(hostConnectionMap)/2 && rn.serverState == raft.Role_Candidate {
								rn.serverState = raft.Role_Follower
								rn.votedFor = -1
								rn.followerChan <- true
							}
						}

						//not get enough vote, be follower or candidate to vote again
						//get majority of vote within election timeout

					}()
				}

				select {
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
					//TODO
					rn.serverState = raft.Role_Follower
					rn.votedFor = -1
				case <-rn.resetChan:
					//TODO

				case <-rn.heartBeatChan:
					rn.serverState = raft.Role_Follower
					rn.votedFor = -1
				case <-rn.leaderChan:
					rn.serverState = raft.Role_Leader
					rn.currentTerm += 1

					go func() {
						time.Sleep(time.Duration(rn.electionTimeout) * time.Millisecond)
						for len(rn.leaderChan) > 0 {
							<-rn.leaderChan
						}
					}()

				}

				//collect and count votes we have got
				//different raftNode return function in different time or even not reply
				//use go routine (concurrently receive
				//set timeout, if timeout, drop

				//vote reply: RequestVoteReply
				//if term is not current term, not valid, candidate of last term

			//need invoke appendEntries of other nodes
			case raft.Role_Leader:
				//TODO

				rn.serverState = raft.Role_Leader

				for hostId, client := range hostConnectionMap {
					//for each raft node, launch go routine
					//sendLog := make([]*raft.LogEntry, 0) //TODO
					if hostId == int32(nodeId) {
						continue
					}

					go func() {

						rn.mu.Lock()

						r, err := client.AppendEntries(
							ctx,
							&raft.AppendEntriesArgs{
								From:         int32(nodeId), //id of our own(leader)
								To:           int32(hostId), //id of other node(follower)
								Term:         rn.currentTerm,
								LeaderId:     int32(nodeId),
								PrevLogIndex: 0,   //int32(len(rn.log) - 1),
								PrevLogTerm:  0,   //rn.log[len(rn.log)-1].GetTerm(),
								Entries:      nil, //not empty send to follower, many logs
								LeaderCommit: 0,   //rn.commitIndex,
							},
						)

						rn.mu.Unlock()

						//No log, heartbeat
						//successfully get return value
						if err == nil && r.Success == true {
							//check whether the follower has received the logs
							//if majority of the raft nodes have received the logs
							//commit it
							rn.followerChan <- true
							rn.matchIndex[hostId] = r.MatchIndex

							/*
								//check each log if it received by follower
								for i := rn.nextIndex[hostId]; i <= rn.nextIndex[hostId]+int32(len(sendLog)); i++ {
									//latest index to commit
									if i == rn.commitIndex+1 {
										//add a lock TODO
										rn.mu.Lock()
										rn.commitNum++
										rn.mu.Unlock()
										// majority commited
										if rn.commitNum == int32(len(hostConnectionMap)/2) {
											//use a channel set
											rn.commitChan <- true
											rn.commitNum = 0

											//log order important, come first, commit first
										} else {
											//TODO
										}
									}
									//rn.commitNum := 0 //how many received the log
								}*/
						} else if r.Term > rn.currentTerm {
							rn.mu.Lock()

							rn.serverState = raft.Role_Follower
							rn.votedFor = -1
							rn.voteCount = 0
							for len(rn.leaderChan) > 0 {
								<-rn.leaderChan
							}

							rn.mu.Unlock()
						}

					}()

					time.Sleep(time.Duration(heartBeatInterval))

				}

				select {
				//timer for heartbeat, after time out, send heart beat again
				case <-time.After(time.Duration(rn.heartBeatInterval) * time.Millisecond):
					rn.heartBeatChan <- true
				case <-rn.resetChan:
					//empty
				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!

	log.Printf("Receive propose from client")
	var ret raft.ProposeReply
	/*

		//only leader can accept new operations
		if rn.serverState == raft.Role_Leader {
			ret.CurrentLeader = rn.votedFor //nodeId
			ret.Status = ret.Status_OK

			/*
				//append log to local logs
				rn.log = append(rn.log,&raft.LogEntry{
					//TODO args
				})<- rn.commitChan	//receive sth from receive channel*/
	/*
			//apply to kv store
			if args.Op == raft.Operation_Put {
				//PUT assign new value, overwrite value
				rn.kvstore[args.Key] = args.V
			} else if args.Op == raft.Operation_Delete {
				//check whether the key exist in our current kvstore, otherwise invalid operation
				//if it exist delete
				//otherwise, key not exist in kvstore, return key not found  ret.Status = raft.Status_KeyNotFound
			}
		} else {
			ret.CurrentLeader = rn.votedFor
			ret.Status = raft.Status_WrongNode
		}*/

	//return when operation committed or not leader

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key

//get key,
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	//check whether key in kv store or not

	var ret raft.GetValueReply
	/*
		//add lock
		if val, ok := rn.kvstore[args.Key]; ok {
			//ok, key in kv store
			ret.V = val
			ret.Status = raft.Status_KeyFound
		} else {
			//TODO
		}*/
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!

	var reply raft.RequestVoteReply

	rn.mu.Lock()

	reply.From = args.To //follower
	reply.To = args.From //candidate

	reply.VoteGranted = false

	if rn.votedFor != -1 {
		reply.VoteGranted = false
	}

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
	}

	//if candidate term newer than me, vote for him
	if args.Term > rn.currentTerm {
		rn.votedFor = args.CandidateId //From or -1, -1 not vote for anyone
		reply.VoteGranted = true
		rn.currentTerm = args.Term
		rn.resetChan <- true
		rn.grantedVoteChan <- true

		//if my state is not follower, change to follower
		if rn.serverState != raft.Role_Follower {
			rn.serverState = raft.Role_Follower
			//TODO:tell the main function that I have changed the role
			//TODO:use channel to send msg
			//rn.resetChan <- true
			rn.followerChan <- true
		}
	}

	reply.Term = rn.currentTerm //I have updated my term already if the candidate term is new

	if args.Term == rn.currentTerm && args.LastLogIndex >= int32(len(rn.log)-1) {
		reply.VoteGranted = true
		rn.votedFor = args.CandidateId
		rn.grantedVoteChan <- true

		/*
			rn.serverState = raft.Role_Follower
			rn.followerChan <- true
			rn.grantedVoteChan <- true
			rn.resetChan <- true*/
	}

	rn.mu.Unlock()

	/*
		reply.Term = rn.currentTerm
		rn.mu.Lock()

		if rn.votedFor != -1 {
			reply.VoteGranted = false
		} else {
			if args.Term > rn.currentTerm {
				reply.VoteGranted = true
				rn.votedFor = args.From
			} else if args.Term == rn.currentTerm && args.LastLogIndex >= int32(len(rn.log)-1) {
				reply.VoteGranted = true
				rn.votedFor = args.From
			} else {
				reply.VoteGranted = false
			}
		}

		rn.mu.Unlock()
	*/

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this

	//can carry empty log or any log
	//heartbeat, let follower know leader is alive, otherwise think leader dead if timeout
	//transfer from leader to follower
	var reply raft.AppendEntriesReply
	rn.mu.Lock()
	defer rn.mu.Unlock()

	/*
		reply.From = args.To
		reply.To = args.From
		reply.Success = false

		//check whether accept this log or not
		//may be from old leader, not accept his log
		if args.Term < rn.currentTerm {
			reply.Term = rn.currentTerm
		}

		rn.heartBeatChan <- true

		//if receive from a new leader of future term, recongnize him as leader
		if args.Term > rn.currentTerm {
			rn.votedFor = args.From
			rn.currentTerm = args.Term
			//change our role to follower
			rn.serverState = raft.Role_Follower
			//if you are leader, do you also need to reset the timer?

			rn.resetChan <- true
		}
		reply.Term = args.Term

		/*
			if args.PrevLogIndex >= int32(len(rn.log)-1) {
				reply.Success = false
			} else {
				if rn.log[args.PrevLogIndex].Term < args.PrevLogTerm {
					reply.Success = false
				} else {
					for i := 0; i < len(args.Entries); i++ {
						index := int(args.PrevLogIndex) + i + 1

						if index < len(rn.log) {
							rn.log[index] = args.Entries[i]
						} else {
							rn.log = append(rn.log, args.Entries[i])
						}
					}
				}
			}

					if args.PrevLogIndex >= int32(len(rn.log)) {
						reply.Success = false
					}
					if args.PrevLogIndex < int32(len(rn.log)) && rn.log[args.PrevLogIndex].GetTerm() < args.PrevLogTerm {
						reply.Success = false
					}


				if reply.Success {
					if args.LeaderCommit > rn.commitIndex {
						newIndex := Min(args.LeaderCommit, int32(len(rn.log)-1))

						if newIndex < int32(len(rn.log)) {
							for i := rn.commitIndex + 1; i <= newIndex; i++ {
								//TODO
							}
						}
					}
				}
	*/
	reply.Success = true
	//if it is success, you need to update the local logs
	//TODO

	/*
		//apply the committed log by comparing the commitIndex and the LeaderCommit
		//follower check LeaderCommit ,
		if args.LeaderCommit > rn.commitIndex {
			//leader commit we not commit
			//apply the commited logs TODO
			rn.commitIndex = Min(args.LeaderCommit, int32(len(rn.log)-1))

		}

		reply.MatchIndex = rn.commitIndex*/
	reply.MatchIndex = 0

	return &reply, nil
}

func Min(a int32, b int32) int32 {
	if a <= b {
		return a
	}

	return b
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!  //change timer, send sth to channel
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.Timeout //new Timeout provided by timer, assign timeout to
	rn.resetChan <- true              //tell the timer to be reset
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.Interval
	rn.resetChan <- true
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
