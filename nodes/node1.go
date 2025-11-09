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
