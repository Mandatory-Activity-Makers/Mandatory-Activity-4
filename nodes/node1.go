package main

import (
	proto "CsService/grpc"
	"context"

	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	proto.UnimplementedCsServiceServer

	node_id int64
	seq_nr  int64
	port    string

	server  *grpc.Server
	clients map[int64]proto.CsServiceClient
}

// NewNode returns a new Node struct.
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
func (n *Node) DialOtherNodes(peers map[int64]string) error {
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

func (n *Node) Request(ctx context.Context, req *proto.NodeRequest) (*proto.NodeResponse, error) {
	// Your Ricart-Agrawala logic here
	// This will be the same for all nodes

	return &proto.NodeResponse{
		PermissionGranted: true,
		NodeId:            n.node_id,
		SeqNr:             req.SeqNr,
	}, nil
}
