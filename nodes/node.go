package main

import (
	"fmt"
	"sync"
	"time"

	proto "CsService/grpc"
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ANSI color codes
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
)

// Node represents a single process participating in the Ricart-Agrawala mutual exclusion algorithm.
type Node struct {
	proto.UnimplementedCsServiceServer
	// CONSTANT
	node_id int64 // This node's unique number
	N       int   // The number of nodes in the network

	// INTEGER
	Our_Sequence_Number     int64 // The sequence number chosen by a request originating at this node
	Highest_Sequence_Number int   // initial (0) The highest sequence number seen in any REQUEST message sent or received
	Outstanding_Reply_Count int   // The number of REPLY messages still expected

	// BOOLEAN
	Requesting_Critical_Section bool              // initial (FALSE) TRUE when this node is requesting access to its critical section
	Reply_Deferred              []bool            // [1:N] initial (FALSE) Reply_Deferred[j] is TRUE when this node is deferring a REPLY to j's REQUEST message
	reply_channels              map[int]chan bool // channel for each node that might be deferred

	mu      sync.Mutex
	port    string // localhost port
	server  *grpc.Server
	clients map[int]proto.CsServiceClient
}

// Helper function for Colors
func containsPrefix(msg, prefix string) bool {
	return len(msg) >= len(prefix) && msg[:len(prefix)] == prefix
}

// logPrint prints color-coded log messages with a short delay to simulate real process timing.
func logPrint(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)

	// Assign unique colors for each tag
	switch {
	case containsPrefix(msg, "[ERROR]"):
		msg = ColorRed + msg + ColorReset
	case containsPrefix(msg, "[REPLY]"):
		msg = "\033[38;5;46m" + msg + ColorReset // bright green
	case containsPrefix(msg, "[REQUEST]"):
		msg = "\033[38;5;39m" + msg + ColorReset // bright blue-cyan
	case containsPrefix(msg, "[GRANT]"):
		msg = "\033[38;5;208m" + msg + ColorReset // orange
	case containsPrefix(msg, "[DEFER]"):
		msg = "\033[38;5;226m" + msg + ColorReset // yellow
	case containsPrefix(msg, "[DEFERRED]"):
		msg = "\033[38;5;220m" + msg + ColorReset // gold
	case containsPrefix(msg, "[SEND-DEFERRED]"):
		msg = "\033[38;5;51m" + msg + ColorReset // aqua blue
	case containsPrefix(msg, "[ENTER]"):
		msg = "\033[38;5;141m" + msg + ColorReset // light purple
	case containsPrefix(msg, "[RELEASE]"):
		msg = "\033[38;5;201m" + msg + ColorReset // pink
	case containsPrefix(msg, "[SERVER]"):
		msg = "\033[38;5;33m" + msg + ColorReset // deep blue
	case containsPrefix(msg, "[DIAL]"):
		msg = "\033[38;5;45m" + msg + ColorReset // teal
	case containsPrefix(msg, "[INIT]"):
		msg = "\033[38;5;14m" + msg + ColorReset // sky blue
	case containsPrefix(msg, "[MAIN]"):
		msg = "\033[38;5;240m" + msg + ColorReset // gray
	case containsPrefix(msg, "[RECV]"):
		msg = "\033[38;5;202m" + msg + ColorReset // bright orange-red
	default:
		msg = ColorWhite + msg + ColorReset // fallback for untagged messages
	}

	// Ensure newline between messages
	if len(msg) == 0 || msg[len(msg)-1] != '\n' {
		msg += "\n"
	}
	fmt.Print(msg)
	time.Sleep(100 * time.Millisecond)
}

// NewNode returns a new Node struct.
//
// Makes for easier struct initialization.
func NewNode(id int64, N int, port string) *Node {
	node := &Node{
		node_id:                     id,
		N:                           N,
		Our_Sequence_Number:         0,
		Highest_Sequence_Number:     0,
		Outstanding_Reply_Count:     N - 1,
		Requesting_Critical_Section: false,
		Reply_Deferred:              make([]bool, N+1),

		port:           port,
		server:         grpc.NewServer(),
		clients:        make(map[int]proto.CsServiceClient),
		reply_channels: make(map[int]chan bool),
	}

	// Pre-create a channel for each possible node
	for i := 1; i <= N; i++ {
		if int64(i) != id { // Don't create for yourself
			node.reply_channels[int(i)] = make(chan bool, 1) // buffered
		}
	}

	logPrint("[INIT] Node %d initialized with port %s and %d total nodes\n", node.node_id, port, N)
	return node
}

// StartServer returns nil if the server startup
// did not create any errors
//
// The function is only accessible by Node structs.
//
// It creates a server stump where it listens to
// incoming dials from other clients
// and also registers it as a server.
func (n *Node) StartServer() error {
	logPrint("[SERVER] Node %d starting gRPC server on %s\n", n.node_id, n.port)
	lis, err := net.Listen("tcp", n.port)
	if err != nil {
		return err
	}

	proto.RegisterCsServiceServer(n.server, n)

	go func() {
		logPrint("[SERVER] Node %d listening for incoming connections...\n", n.node_id)
		if err := n.server.Serve(lis); err != nil {
			log.Fatalf("Node %d server error: %v", n.node_id, err)
		}
	}()
	return nil
}

// DialOtherNodes returns nil if no dials were faulty
//
// # First it creates connection (conn) with the server
//
// # Secondly it creates a new client from that connection
//
// # Thirdly it stores the client value in the Node's map
func (n *Node) DialOtherNodes(peers map[int]string) error {
	logPrint("[DIAL] Node %d dialing other nodes...\n", n.node_id)
	for peerID, address := range peers {
		logPrint("[DIAL] Node %d attempting connection to Node %d at %s\n", n.node_id, peerID, address)
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logPrint("[ERROR] Node %d failed to connect to Node %d: %v\n", n.node_id, peerID, err)
			return err
		}

		client := proto.NewCsServiceClient(conn)
		n.clients[peerID] = client
		logPrint("[DIAL] Node %d successfully connected to Node %d\n", n.node_id, peerID)
	}
	logPrint("[DIAL] Node %d finished dialing all peers\n", n.node_id)
	return nil
}

// RequestCriticalSection initiates a request to enter
// the critical section according to the Ricart-Agrawala algorithm.
//
// It broadcasts REQUEST messages to all other nodes and
// waits for all REPLY messages before entering the CS.
func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.Requesting_Critical_Section = true
	n.Outstanding_Reply_Count = n.N - 1
	n.Our_Sequence_Number = int64(n.Highest_Sequence_Number) + 1
	n.Highest_Sequence_Number = int(n.Our_Sequence_Number)
	ourSeq := n.Our_Sequence_Number // Save for use outside lock
	n.mu.Unlock()

	// Send to all other nodes
	for peerID, client := range n.clients {
		logPrint("[REQUEST] Node %d requesting CS from Node %d, (Seq=%d)\n", n.node_id, peerID, ourSeq)
		resp, err := client.Request(context.Background(), &proto.NodeRequest{
			NodeId: n.node_id,
			SeqNr:  ourSeq,
		})
		n.mu.Lock()
		time.Sleep(time.Second * 1)
		n.mu.Unlock()
		if err == nil && resp.PermissionGranted == true {
			n.mu.Lock()
			n.Outstanding_Reply_Count--
			logPrint("[REPLY] Node %d received REPLY from Node %d (Remaining=%d)\n",
				n.node_id, peerID, n.Outstanding_Reply_Count)
			n.mu.Unlock()
		} else if err != nil {
			logPrint("[ERROR] Node %d request to Node %d failed: %v\n", n.node_id, peerID, err)
		}
	}

	// Wait for all replies
	for {
		n.mu.Lock()
		count := n.Outstanding_Reply_Count
		n.mu.Unlock()

		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n.Outstanding_Reply_Count == 0 { // 2 is the max nr of replies (hardcoded)
		// Enter CS (ONLY ONCE!)
		logPrint("[ENTER] Node %d entering Critical Section, (Seq=%d)\n", n.node_id, n.Our_Sequence_Number)
		time.Sleep(1 * time.Second)
		n.ReleaseCriticalSection() // release the critical section after accessing it
	}
}

// ReleaseCriticalSection resets the requesting flag,
// and sends deferred REPLY messages to nodes that were waiting.
func (n *Node) ReleaseCriticalSection() {
	n.mu.Lock()
	n.Requesting_Critical_Section = false
	n.mu.Unlock()

	for j := 1; j <= n.N; j++ {
		if n.Reply_Deferred[j] {
			logPrint("[DEFERRED] Node %d sending REPLY to deferred Node %d\n", n.node_id, j)
			n.Reply_Deferred[j] = false
			n.reply_channels[j] <- true
		}
	}
	logPrint("[RELEASE] Node %d released Critical Section\n", n.node_id)
}

// Request handles an incoming REQUEST message from another node.
//
// It decides whether to immediately grant permission
// or defer the REPLY based on the Ricart-Agrawala conditions.
func (n *Node) Request(ctx context.Context, req *proto.NodeRequest) (*proto.NodeResponse, error) {
	logPrint("[RECV] Node %d received REQUEST from Node %d, (Seq=%d)\n", n.node_id, req.NodeId, req.SeqNr)

	n.mu.Lock()
	if req.SeqNr > int64(n.Highest_Sequence_Number) {
		n.Highest_Sequence_Number = int(req.SeqNr)
	}
	requesting := n.Requesting_Critical_Section
	ourSeq := n.Our_Sequence_Number
	ourID := n.node_id
	n.mu.Unlock()

	if !requesting ||
		req.SeqNr < ourSeq && requesting ||
		req.SeqNr == ourSeq && req.NodeId < ourID {
		// reply OK
		logPrint("[GRANT] Node %d granting REPLY to Node %d immediately\n", n.node_id, req.NodeId)
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	} else {
		logPrint("[DEFER] Node %d deferring REPLY to Node %d\n", n.node_id, req.NodeId)
		n.mu.Lock()
		n.Reply_Deferred[req.NodeId] = true // defers the incoming requesting node
		n.mu.Unlock()

		<-n.reply_channels[int(req.NodeId)] // blocks until it receives a signal from requesting node

		logPrint("[SEND-DEFERRED] Node %d sending deferred REPLY to Node %d\n", n.node_id, req.NodeId)
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	}
}

var wg sync.WaitGroup

func main() {
	logPrint("[MAIN] Initializing nodes...")
	// Create 3 nodes
	node1 := NewNode(1, 3, "localhost:5001")
	node2 := NewNode(2, 3, "localhost:5002")
	node3 := NewNode(3, 3, "localhost:5003")

	node1_peers := map[int]string{2: "localhost:5002", 3: "localhost:5003"}
	node2_peers := map[int]string{1: "localhost:5001", 3: "localhost:5003"}
	node3_peers := map[int]string{1: "localhost:5001", 2: "localhost:5002"}

	logPrint("[MAIN] Starting all servers...")
	node1.StartServer()
	node2.StartServer()
	node3.StartServer()

	logPrint("[MAIN] Connecting all nodes...")
	node1.DialOtherNodes(node1_peers)
	node2.DialOtherNodes(node2_peers)
	node3.DialOtherNodes(node3_peers)

	time.Sleep(2 * time.Second)
	logPrint("[MAIN] All servers started and connected.")
	logPrint("=========================================\nRUNNING ALGORITHM\n=========================================")

	var wg sync.WaitGroup
	nr_of_times_entering_cs := 1 // change this to test longer logs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nr_of_times_entering_cs; i++ {
			node1.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nr_of_times_entering_cs; i++ {
			node2.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nr_of_times_entering_cs; i++ {
			node3.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Wait()
	logPrint("[MAIN] All nodes completed their CS requests!")
}
