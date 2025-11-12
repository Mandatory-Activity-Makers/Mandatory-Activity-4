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
	Reply_Deferred              []bool            // [1:N] initial (FALSE) Reply_Deferred [j] is TRUE when this node is deferring a REPLY to j's REQUEST message
	reply_channels              map[int]chan bool // channel for each node that might be deferred

	mu      sync.Mutex
	port    string // localhost port
	server  *grpc.Server
	clients map[int]proto.CsServiceClient
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
	for i := 1; i <= N-1; i++ {
		if int64(i) != id { // Don't create for yourself
			node.reply_channels[int(i)] = make(chan bool, 1) // buffered
		}
	}
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
	lis, err := net.Listen("tcp", n.port)
	if err != nil {
		return err
	}

	proto.RegisterCsServiceServer(n.server, n)

	go func() {
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
	for peerID, address := range peers {

		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		client := proto.NewCsServiceClient(conn)

		n.clients[peerID] = client
	}
	return nil
}

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.Requesting_Critical_Section = true
	n.Outstanding_Reply_Count = n.N - 1
	// CHANGE THESE TWO LINES:
	n.Our_Sequence_Number = int64(n.Highest_Sequence_Number) + 1
	n.Highest_Sequence_Number = int(n.Our_Sequence_Number)
	ourSeq := n.Our_Sequence_Number // Save for use outside lock
	n.mu.Unlock()

	// Send to all other nodes
	for peerID, client := range n.clients {
		resp, err := client.Request(context.Background(), &proto.NodeRequest{
			NodeId: n.node_id,
			SeqNr:  ourSeq,
		})
		fmt.Printf("Node %d is Requesting CS from Node %d at Time %d\n", n.node_id, peerID, ourSeq)
		if err == nil && resp.PermissionGranted == true {
			n.mu.Lock()
			n.Outstanding_Reply_Count--
			n.mu.Unlock()
		}
	}

	for {
		n.mu.Lock()
		count := n.Outstanding_Reply_Count
		n.mu.Unlock()

		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now enter CS
	fmt.Printf("Node %d accessed the critical section at time %d\n", n.node_id, n.Our_Sequence_Number)
	time.Sleep(1 * time.Second)

	// Release CS
	n.ReleaseCriticalSection()
}

func (n *Node) ReleaseCriticalSection() {
	n.Requesting_Critical_Section = false
	for j := 1; j < n.N-1; j++ {
		if n.Reply_Deferred[j] {
			n.Reply_Deferred[j] = false
			n.reply_channels[j] <- true // send a reply to node j and release the critical sectiong
		}
	}
	fmt.Printf("Node %d released the Critical Section\n", n.node_id)
}

func (n *Node) Request(ctx context.Context, req *proto.NodeRequest) (*proto.NodeResponse, error) {
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
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	} else {
		n.mu.Lock()
		n.Reply_Deferred[req.NodeId] = true // defers the incoming requesting node
		n.mu.Unlock()

		<-n.reply_channels[int(req.NodeId)] // blocks until it receives a signal from requesting node

		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	}
}

var wg sync.WaitGroup

func main() {
	// Create 3 nodes
	node1 := NewNode(1, 3, "localhost:5001")
	node2 := NewNode(2, 3, "localhost:5002")
	node3 := NewNode(3, 3, "localhost:5003")
	node1_peers := map[int]string{
		2: "localhost:5002",
		3: "localhost:5003",
	}
	node2_peers := map[int]string{
		1: "localhost:5001",
		3: "localhost:5003",
	}
	node3_peers := map[int]string{
		1: "localhost:5001",
		2: "localhost:5002",
	}
	// Start all servers
	node1.StartServer()
	node2.StartServer()
	node3.StartServer()

	// Connect them to each other
	node1.DialOtherNodes(node1_peers)
	node2.DialOtherNodes(node2_peers)
	node3.DialOtherNodes(node3_peers)

	time.Sleep(2 * time.Second)

	wg.Add(1)
	// Make each node request CS 3 times (how?)
	// Make each node request CS 3 times concurrently
	go func() {
		for i := 0; i < 3; i++ {
			// Node 1 requests
			// Maybe add a small delay between requests?
			node1.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()
	wg.Done()
	wg.Add(1)
	go func() {
		for i := 0; i < 3; i++ {
			// Node 2 requests
			node2.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()
	wg.Done()
	wg.Add(1)
	go func() {
		for i := 0; i < 3; i++ {
			// Node 3 requests
			node3.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()
	wg.Done()
	wg.Wait()

	// Keep program running
	select {}
}
