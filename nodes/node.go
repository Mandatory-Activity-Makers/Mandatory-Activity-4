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
	node_id int64
	N       int

	Our_Sequence_Number     int64
	Highest_Sequence_Number int
	Outstanding_Reply_Count int

	Requesting_Critical_Section bool
	Reply_Deferred              []bool
	reply_channels              map[int]chan bool

	mu      sync.Mutex
	port    string
	server  *grpc.Server
	clients map[int]proto.CsServiceClient
}

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

	for i := 1; i <= N; i++ {
		if int64(i) != id {
			node.reply_channels[int(i)] = make(chan bool, 1)
		}
	}

	fmt.Printf("[INIT] Node %d initialized with port %s and %d total nodes\n", node.node_id, port, N)
	return node
}

func (n *Node) StartServer() error {
	fmt.Printf("[SERVER] Node %d starting gRPC server on %s\n", n.node_id, n.port)
	lis, err := net.Listen("tcp", n.port)
	if err != nil {
		return err
	}

	proto.RegisterCsServiceServer(n.server, n)

	go func() {
		fmt.Printf("[SERVER] Node %d listening for incoming connections...\n", n.node_id)
		if err := n.server.Serve(lis); err != nil {
			log.Fatalf("Node %d server error: %v", n.node_id, err)
		}
	}()
	return nil
}

func (n *Node) DialOtherNodes(peers map[int]string) error {
	fmt.Printf("[DIAL] Node %d dialing other nodes...\n", n.node_id)
	for peerID, address := range peers {
		fmt.Printf("[DIAL] Node %d attempting connection to Node %d at %s\n", n.node_id, peerID, address)
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("[ERROR] Node %d failed to connect to Node %d: %v\n", n.node_id, peerID, err)
			return err
		}

		client := proto.NewCsServiceClient(conn)
		n.clients[peerID] = client
		fmt.Printf("[DIAL] Node %d successfully connected to Node %d\n", n.node_id, peerID)
	}
	fmt.Printf("[DIAL] Node %d finished dialing all peers\n", n.node_id)
	return nil
}

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.Requesting_Critical_Section = true
	n.Outstanding_Reply_Count = n.N - 1
	n.Our_Sequence_Number = int64(n.Highest_Sequence_Number) + 1
	n.Highest_Sequence_Number = int(n.Our_Sequence_Number)
	ourSeq := n.Our_Sequence_Number
	fmt.Printf("[REQUEST] Node %d requesting CS (Seq=%d)\n", n.node_id, ourSeq)
	n.mu.Unlock()

	for peerID, client := range n.clients {
		fmt.Printf("[SEND] Node %d sending REQUEST to Node %d (Seq=%d)\n", n.node_id, peerID, ourSeq)
		resp, err := client.Request(context.Background(), &proto.NodeRequest{
			NodeId: n.node_id,
			SeqNr:  ourSeq,
		})
		n.mu.Lock()
		fmt.Printf("[STATE] Node %d awaiting REPLY from Node %d\n", n.node_id, peerID)
		time.Sleep(time.Second * 1)
		n.mu.Unlock()
		if err == nil && resp.PermissionGranted == true {
			n.mu.Lock()
			n.Outstanding_Reply_Count--
			fmt.Printf("[REPLY] Node %d received REPLY from Node %d (Remaining=%d)\n",
				n.node_id, peerID, n.Outstanding_Reply_Count)
			n.mu.Unlock()
		} else if err != nil {
			fmt.Printf("[ERROR] Node %d request to Node %d failed: %v\n", n.node_id, peerID, err)
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

	if n.Outstanding_Reply_Count == 0 {
		fmt.Printf("[ENTER] Node %d entering Critical Section (Seq=%d)\n", n.node_id, n.Our_Sequence_Number)
		time.Sleep(1 * time.Second)
		n.ReleaseCriticalSection()
	}
}

func (n *Node) ReleaseCriticalSection() {
	fmt.Printf("[RELEASE] Node %d releasing Critical Section\n", n.node_id)
	n.mu.Lock()
	n.Requesting_Critical_Section = false
	n.mu.Unlock()

	for j := 1; j <= n.N; j++ {
		if n.Reply_Deferred[j] {
			fmt.Printf("[DEFERRED] Node %d sending deferred REPLY to Node %d\n", n.node_id, j)
			n.Reply_Deferred[j] = false
			n.reply_channels[j] <- true
		}
	}
	fmt.Printf("[RELEASE] Node %d finished releasing Critical Section\n", n.node_id)
}

func (n *Node) Request(ctx context.Context, req *proto.NodeRequest) (*proto.NodeResponse, error) {
	fmt.Printf("[RECV] Node %d received REQUEST from Node %d (Seq=%d)\n", n.node_id, req.NodeId, req.SeqNr)

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
		fmt.Printf("[GRANT] Node %d granting REPLY to Node %d immediately\n", n.node_id, req.NodeId)
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	} else {
		fmt.Printf("[DEFER] Node %d deferring REPLY to Node %d\n", n.node_id, req.NodeId)
		n.mu.Lock()
		n.Reply_Deferred[req.NodeId] = true
		n.mu.Unlock()

		<-n.reply_channels[int(req.NodeId)]
		fmt.Printf("[SEND-DEFERRED] Node %d sending deferred REPLY to Node %d\n", n.node_id, req.NodeId)
		return &proto.NodeResponse{
			PermissionGranted: true,
			NodeId:            n.node_id,
			SeqNr:             req.SeqNr,
		}, nil
	}
}

var wg sync.WaitGroup

func main() {
	fmt.Println("[MAIN] Initializing nodes...")
	node1 := NewNode(1, 3, "localhost:5001")
	node2 := NewNode(2, 3, "localhost:5002")
	node3 := NewNode(3, 3, "localhost:5003")

	node1_peers := map[int]string{2: "localhost:5002", 3: "localhost:5003"}
	node2_peers := map[int]string{1: "localhost:5001", 3: "localhost:5003"}
	node3_peers := map[int]string{1: "localhost:5001", 2: "localhost:5002"}

	fmt.Println("[MAIN] Starting all servers...")
	node1.StartServer()
	node2.StartServer()
	node3.StartServer()

	fmt.Println("[MAIN] Connecting all nodes...")
	node1.DialOtherNodes(node1_peers)
	node2.DialOtherNodes(node2_peers)
	node3.DialOtherNodes(node3_peers)

	time.Sleep(2 * time.Second)
	fmt.Println("[MAIN] All servers started and connected.")

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			node1.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			node2.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			node3.RequestCriticalSection()
			time.Sleep(time.Second * 2)
		}
	}()

	wg.Wait()
	fmt.Println("[MAIN] All nodes completed their CS requests!")
}
