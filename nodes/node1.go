package main

import (
	proto "CsService/grpc"

	"log"
	"net"

	"google.golang.org/grpc"
)

type Node struct {
	proto.UnimplementedCsServiceServer

	node_id int64
	seq_nr  int64
	port    string

	server  *grpc.Server
	clients map[int64]proto.CsServiceClient
}

// NewNode(...) returns a new Node struct.
//
// Makes for easier struct initialization.
func NewNode(id int64, port string) *Node {
	node := &Node{
		node_id: id,
		seq_nr:  0,
		port:    port,
		server:  grpc.NewServer(),
		clients: make(map[int64]proto.CsServiceClient),
	}
	return node
}

// Start() returns nil if the server startup
// did not create any errors
//
// The function is only accessible by Node structs.
//
// It creates a server stump where it listens to
// incoming dials from other clients
// and also registers it as a server.
func (n *Node) Start() error {
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
