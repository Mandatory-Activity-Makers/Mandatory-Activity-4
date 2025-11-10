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
		Reply_Deferred:              make([]bool, N),

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
	n.Requesting_Critical_Section = true
	n.Our_Sequence_Number++
	// Send to all other nodes
	for peerID, client := range n.clients {
		resp, err := client.Request(context.Background(), &proto.NodeRequest{
			NodeId: n.node_id,
			SeqNr:  n.Our_Sequence_Number,
		})
		fmt.Print("Node %s is Requesting CS from Node %s at Time %d", n.node_id, peerID, n.Our_Sequence_Number)
		if err == nil && resp.PermissionGranted == true {
			n.Outstanding_Reply_Count-- // Got a reply!
		}
	}

	if 0 == n.Outstanding_Reply_Count { // 2 is the max nr of replies (hardcoded)
		fmt.Printf("Node %s accessed the critical section", n.node_id) // write to txt file new line (not required)
		time.Sleep(1000)
	}
}

func (n *Node) ReleaseCriticalSection() {
	n.Requesting_Critical_Section = false
	for j := 1; j < n.N; j++ {
		if n.Reply_Deferred[j] {
			n.Reply_Deferred[j] = false
			n.reply_channels[j] <- true // send a reply to node j and release the critical sectiong
		}
	}
}

func (n *Node) Send_Message(ctx context.Context, res *proto.NodeResponse, id int) {

}

func (n *Node) Reply(ctx context.Context, rep *proto.NodeResponse) {

}

func (n *Node) Request(ctx context.Context, req *proto.NodeRequest) (*proto.NodeResponse, error) {
	if !n.Requesting_Critical_Section || req.SeqNr < n.Our_Sequence_Number && n.Requesting_Critical_Section || req.SeqNr == n.Our_Sequence_Number && req.NodeId < n.node_id {
		// reply OK
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	}

}
