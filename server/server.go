package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	proto "main/grpc"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type AuctionServer struct {
	proto.UnimplementedAuctionServer
	port       string
	id         int32
	clk        *int32
	bidders    map[string]string
	currentBid int32
	isOver     bool
	backend    *BackendClient
	mu         sync.Mutex
}

type BackendServer struct {
	proto.UnimplementedBackendServer
	port      string
	id        string
	clk       int32 //Main Clock
	isLeader  bool
	advServer *zeroconf.Server
	mu        sync.Mutex
	client    *BackendClient
}

type BackendClient struct {
	id             string
	clk            *int32
	replicas       map[string]proto.BackendClient
	lastSeen       map[string]time.Time
	mu             sync.Mutex
	leader         proto.BackendClient
	leaderId       string
	isLeader       bool
	lastLeaderSeen time.Time
	server         *BackendServer
	auctionServer  *AuctionServer
}

type ServerNodeInfo struct {
	nodeId   string
	address  string
	isLeader bool
}

func main() {

	var server *AuctionServer = &AuctionServer{}
	server.bidders = make(map[string]string)
	server.currentBid = 0
	server.isOver = false
	server.port = "5001" //Default port

	var backend *BackendServer = &BackendServer{}
	backend.port = "5011"
	backend.clk = 0

	if len(os.Args) > 1 {
		server.port = os.Args[1] //Override default port
	}
	if len(os.Args) > 2 {
		id, _ := strconv.Atoi(os.Args[2])
		server.id = int32(id)
		backend.id = os.Args[2]
	}
	if len(os.Args) > 3 {
		backend.port = os.Args[3]
	}
	if len(os.Args) > 4 {
		backend.isLeader, _ = strconv.ParseBool(os.Args[4])
	}

	backendClient := &BackendClient{
		id:       backend.id,
		replicas: make(map[string]proto.BackendClient),
		lastSeen: make(map[string]time.Time),
		isLeader: backend.isLeader}

	server.backend = backendClient

	//Sync clocks
	backendClient.clk = &backend.clk
	server.clk = &backend.clk

	//Start up and configure logging output to file
	f, err := os.OpenFile("logs/server"+fmt.Sprintf("%d", server.id)+"log"+time.Now().Format("20060102150405")+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	//defer to close when we are done with it.
	defer f.Close()

	//set output of logs to f
	log.SetOutput(f)
	log.Printf("[%d]: Started logging", server.id)

	go server.start_server()
	go backend.start_backend()

	backendClient.server = backend
	backend.client = backendClient
	backendClient.auctionServer = server

	time.Sleep(200 * time.Millisecond)

	backendClient.startPeerDiscovery() //Run in background to discover new server nodes

	for {
		time.Sleep(1000 * time.Millisecond)
	}

}

func (s *AuctionServer) start_server() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterAuctionServer(grpcServer, s)
	log.Printf("Frontend: gRPC server now listening on %s... at logical time: %d \n", s.port, *s.clk)

	go s.startAdvertisingAuction()

	go func() {
		for {
			s.mu.Lock()
			if s.backend.isLeader {
				log.Printf("Clock: %d", *s.clk)
				if *s.clk > int32(100) {
					s.isOver = true
				}
				s.mu.Unlock()
				time.Sleep(4 * time.Second)
			} else {
				s.mu.Unlock()
				time.Sleep(4 * time.Second)
			}

		}
	}()

	grpcServer.Serve(listener)

	defer grpcServer.Stop()
}

func (b *BackendServer) start_backend() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", b.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterBackendServer(grpcServer, b)
	log.Printf("Backend: gRPC server now listening on %s... at logical time: %d \n", b.port, b.clk)
	b.startAdvertising()
	grpcServer.Serve(listener)

	defer grpcServer.Stop()
}

func (s *AuctionServer) Bid(ctx context.Context, amount *proto.Amount) (*proto.Ack, error) {
	s.mu.Lock()
	if !s.isOver {
		if !s.backend.isLeader {
			s.mu.Unlock()
			return s.forwardBid(ctx, amount)
		}
		*s.clk = max(*s.clk, amount.Clock) + 1
		log.Printf("[Node %d] received bid from client: %s (%d)", s.id, amount.ClientId, amount.Amount)
		// Check / register bidder
		_, ok := s.bidders[amount.ClientId]
		if !ok {
			s.bidders[amount.ClientId] = amount.ClientId
		}
		s.mu.Unlock()
		result := s.UpdateBid(amount.Amount)
		return result, nil
	} else {
		clk := *s.clk
		s.mu.Unlock()
		return &proto.Ack{Clock: clk, Ack: proto.AckTypes_FAIL}, nil
	}
}

func (s *AuctionServer) UpdateBid(amount int32) *proto.Ack {
	var result proto.Ack
	s.mu.Lock()
	*s.clk++
	if amount > s.currentBid {
		s.currentBid = amount
		noOfReplicas := len(s.backend.replicas)
		log.Printf("[Node %d] bid: %d", s.id, s.currentBid)
		s.mu.Unlock()
		//Sync with other instances of server
		// asynchronously update replicas
		if noOfReplicas > 0 {
			_, err := s.backend.sendUpdateToReplicas(s.currentBid)
			if err != nil {
				log.Printf("[Node %d] an error occurred %v", s.id, err)
			}
		}
		result = proto.Ack{Ack: proto.AckTypes_SUCCESS}
		return &result
	} else {
		s.mu.Unlock()
		result = proto.Ack{Ack: proto.AckTypes_FAIL}
		return &result
	}
}

func (s *AuctionServer) Result(ctx context.Context, empty *emptypb.Empty) (*proto.AuctionResult, error) {

	s.mu.Lock()
	if s.isOver {
		s.mu.Unlock()
		return &proto.AuctionResult{Clock: *s.clk, Result: s.currentBid, IsOver: s.isOver}, nil
	} else {
		s.mu.Unlock()
		return &proto.AuctionResult{Clock: *s.clk, Result: s.currentBid, IsOver: s.isOver}, nil
	}
}

func (s *AuctionServer) forwardBid(ctx context.Context, amount *proto.Amount) (*proto.Ack, error) {
	// Check if leader is alive
	if s.backend.IsLeaderAlive() {
		// Use a child context with timeout when forwarding
		forwardCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		ack, err := s.backend.leader.Forward(forwardCtx, amount)
		return &proto.Ack{Clock: ack.GetClock(), Ack: ack.GetAck()}, err
	}

	// If leader isn't alive, wait for a new leader to be elected
	for {
		time.Sleep(500 * time.Millisecond)
		if s.backend.IsLeaderAlive() {
			break
		}
	}

	// Now try again with the new leader
	forwardCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	ack, err := s.backend.leader.Forward(forwardCtx, amount)
	s.mu.Lock()
	s.isOver = ack.GetIsOver()
	s.mu.Unlock()
	return &proto.Ack{Clock: ack.GetClock(), Ack: ack.GetAck()}, err
}

// startAdvertising registers zeroconf and keeps a reference
func (s *AuctionServer) startAdvertisingAuction() {
	s.mu.Lock()
	auId := fmt.Sprintf("%d", s.id)
	port, _ := strconv.Atoi(s.port)
	s.mu.Unlock()

	txt := []string{
		"nodeID=" + auId,
	}

	_, err := zeroconf.Register(
		"node-"+auId,
		"_auctionFrontend._tcp",
		"local.",
		port,
		txt,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to advertise node %s: %v", auId, err)
	}
	log.Printf("[Node %s] Advertised frontend on network (port %d)", auId, port)
}

func (c *BackendClient) sendUpdateToReplicas(amount int32) (proto.AckTypes, error) {
	var noOfReplicas int = len(c.replicas)
	c.mu.Lock()
	replicas := make(map[string]proto.BackendClient, len(c.replicas))
	maps.Copy(replicas, c.replicas)
	replySuccessCount := 0
	*c.clk++
	clk := *c.clk
	c.mu.Unlock()
	if len(replicas) > 0 {
		for _, rep := range replicas {

			amtMessage := proto.Amount{Clock: clk, Amount: amount}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			ack, err := rep.TryToUpdateBid(ctx, &amtMessage)
			if err != nil {
				log.Printf("[Node %s] Could not communicate with replica: %v", c.id, err)
				break
			}

			if ack.Ack == proto.AckTypes_SUCCESS {
				log.Printf("[Node %s] Replica updated successfully", c.id)
			}

		}
	} else {
		return proto.AckTypes_FAIL, errors.New("not connected to other replicas")
	}

	if replySuccessCount == noOfReplicas {
		return proto.AckTypes_SUCCESS, nil
	} else {
		return proto.AckTypes_FAIL, nil
	}
}

func (b *BackendServer) TryToUpdateBid(ctx context.Context, amount *proto.Amount) (*proto.Ack, error) {
	b.mu.Lock()
	b.clk = max(amount.Clock, b.clk) + 1
	aSrv := b.client.auctionServer
	aSrv.mu.Lock()
	aSrv.currentBid = amount.GetAmount()
	aSrv.mu.Unlock()
	b.mu.Unlock()
	return &proto.Ack{Clock: int32(b.clk), Ack: proto.AckTypes_SUCCESS}, nil
}

func (c *BackendServer) Forward(ctx context.Context, amount *proto.Amount) (*proto.BackendAck, error) {
	log.Printf("[Node %s] received forward from client from replica: %s", c.id, amount.ClientId)
	c.mu.Lock()
	isOver := c.client.auctionServer.isOver
	c.mu.Unlock()
	ack, err := c.client.auctionServer.Bid(ctx, amount)
	return &proto.BackendAck{Clock: ack.Clock, Ack: ack.GetAck(), IsOver: isOver}, err
}

func (c *BackendClient) startPeerDiscovery() {
	log.Printf("[%s]: Starting peer discovery...", c.id)

	// Run a separate goroutine for leader heartbeat check
	go func() {
		for {
			time.Sleep(10 * time.Second)
			c.IsLeaderAlive()
		}
	}()

	// Run a separate goroutine for replica timeout
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.mu.Lock()
			for k, v := range c.lastSeen {
				if time.Since(v) > 10*time.Second {
					// Node k timed out
					log.Printf("[Node %s] Node %s timed out (last seen %v)", c.id, k, v)

					// Remove it from the map (or mark it dead)
					delete(c.lastSeen, k)
					delete(c.replicas, k)
				}
			}
			c.mu.Unlock()
		}
	}()

	// Continuous peer discovery
	go func() {
		discovered := make(chan ServerNodeInfo)
		go c.discoverBackendNodes(discovered)
		for {
			for peer := range discovered {
				// Check if replica exists under lock
				c.mu.Lock()
				_, exists := c.replicas[peer.nodeId]
				c.mu.Unlock()
				if exists {
					if peer.isLeader && (c.leaderId != peer.nodeId) {
						c.mu.Lock()
						c.leader = c.replicas[peer.nodeId]
						c.leaderId = peer.nodeId
						c.lastLeaderSeen = time.Now()
						c.mu.Unlock()

						log.Printf("[Node %s] Leader updated via TXT: %s", c.id, peer.nodeId)
					} else {
						c.lastSeen[peer.nodeId] = time.Now()
					}

					continue
				}
				conn, err := grpc.NewClient(peer.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("[Node %s] Failed to connect to %s: %v", c.id, peer.address, err)
					continue
				}
				client := proto.NewBackendClient(conn)
				if client == nil {
					log.Printf("[Node %s] gRPC returned nil client??", c.id)
					continue
				}
				c.mu.Lock()
				c.replicas[peer.nodeId] = client

				if peer.isLeader {
					if c.leader == nil {
						c.leader = c.replicas[peer.nodeId]
						c.leaderId = peer.nodeId
						c.lastLeaderSeen = time.Now()
						log.Printf("[Node %s] New leader found %s (%s)", c.id, peer.nodeId, peer.address)
					} else if c.leader != nil && peer.nodeId == c.leaderId {
						c.lastLeaderSeen = time.Now()
						log.Printf("[Node %s] Leader still alive %s (%s)", c.id, peer.nodeId, peer.address)
					}
				} else {
					c.lastSeen[peer.nodeId] = time.Now() //Update seen timestamp
					if c.isLeader {
						*c.clk++
						ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
						cancel() //Timeout
						client.TryToUpdateBid(ctx, &proto.Amount{Amount: c.auctionServer.currentBid, Clock: *c.clk})
					}
				}

				c.mu.Unlock()
			}

			time.Sleep(5 * time.Second) // discovery interval
		}
	}()
}

func (c *BackendClient) IsLeaderAlive() bool {
	c.mu.Lock()
	leaderClient := c.leader
	leaderID := c.leaderId
	c.mu.Unlock()

	if c.leader != nil {
		_, err := leaderClient.Ping(context.Background(), &emptypb.Empty{})
		if err != nil {
			log.Printf("[Node %s] Leader %s unresponsive, triggering election (%v)", c.id, c.leaderId, err)
			c.mu.Lock()
			delete(c.replicas, leaderID)
			c.leader = nil
			c.leaderId = ""
			c.mu.Unlock()
			go c.callForElection()
			return false
		} else {
			c.mu.Lock()
			c.lastLeaderSeen = time.Now() // update heartbeat
			c.mu.Unlock()
			return true
		}
	} else {
		return false //Leader is not set
	}
}

func (c *BackendClient) discoverBackendNodes(discovered chan<- ServerNodeInfo) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		log.Fatalf("[Node %s] Failed to initialize resolver: %v", c.id, err)
	}

	entries := make(chan *zeroconf.ServiceEntry)

	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {
			// skip self
			if entry.Instance == fmt.Sprintf("node-%s", c.id) || len(entry.AddrIPv4) == 0 {
				continue
			}

			addr := fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), entry.Port)

			peerID := entry.Instance[len("node-"):]
			isLeaderTXT := false

			for _, txt := range entry.Text {
				if strings.HasPrefix(txt, "isLeader=") {
					val := strings.TrimPrefix(txt, "isLeader=")
					isLeaderTXT = (val == "true")
					break
				}
			}

			log.Printf("[Node %s] Discovered server node: %s (%s) leader=%v", c.id, peerID, addr, isLeaderTXT)
			discovered <- ServerNodeInfo{nodeId: peerID, address: addr, isLeader: isLeaderTXT}
		}
	}(entries)

	// Long-lived browse context
	// Start browsing
	ctx := context.Background()
	go func() {
		for {
			if err := resolver.Browse(ctx, "_auctionBackend._tcp", "local.", entries); err != nil {
				log.Printf("[Node %s] Browse error: %v", c.id, err)
			}
			time.Sleep(5 * time.Second) // Retry interval
		}
	}()
}

// startAdvertising registers zeroconf and keeps a reference
func (b *BackendServer) startAdvertising() {
	txt := []string{
		"nodeID=" + b.id,
		"isLeader=" + fmt.Sprintf("%t", b.isLeader),
	}

	port, _ := strconv.Atoi(b.port)

	server, err := zeroconf.Register(
		"node-"+b.id,
		"_auctionBackend._tcp",
		"local.",
		port,
		txt,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to advertise node %s: %v", b.id, err)
	}

	b.mu.Lock()
	b.advServer = server
	b.mu.Unlock()
	log.Printf("[Node %s] Advertised on network (port %d) as leader? %t", b.id, port, b.isLeader)
}

func (b *BackendServer) UpdateLeaderStatus(isLeader bool) {
	b.mu.Lock()
	b.isLeader = isLeader
	adv := b.advServer
	b.mu.Unlock()

	adv.SetText([]string{
		"nodeID=" + b.id,
		"isLeader=" + fmt.Sprintf("%t", b.isLeader),
	})
	log.Printf("[Node %s] Updated leader TXT to %t", b.id, b.isLeader)

}

func (c *BackendClient) callForElection() {
	c.mu.Lock()
	nodeId, _ := strconv.Atoi(c.id)
	if len(c.replicas) <= 0 {
		log.Printf("[Node %s] No other replicas in network, I will promote myself to leader! ", c.id)
		c.isLeader = true
		c.mu.Unlock()
		c.server.UpdateLeaderStatus(true)
		return
	} else {
		// Copy replicas map
		replicas := make(map[string]proto.BackendClient, len(c.replicas))
		maps.Copy(replicas, c.replicas)
		c.mu.Unlock()

		log.Printf("[Node %s] Calling an election between %d nodes", c.id, len(replicas))
		alive := false
		c.mu.Lock()
		*c.clk++ //Update clock once, as we "broadcast" once...
		c.mu.Unlock()
		var wg sync.WaitGroup
		for replicaId, replica := range replicas {
			replicaId_string, _ := strconv.Atoi(replicaId)
			if replicaId_string <= nodeId {
				// Skip replicas with lower or equal IDs
				continue
			}
			wg.Add(1) //Increment the "work-to-be-done" counter
			go func(r proto.BackendClient, rid string) {
				defer wg.Done() //mark the job as done
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel() //Timeout
				answer, err := r.Election(ctx, &proto.Message{Id: fmt.Sprintf("%d", nodeId), Clock: *c.clk})
				if err != nil {
					log.Printf("[Node %s] Failed to call Election on replica %s: %v", c.id, rid, err)
					return
				}

				if answer != nil {
					//The replica is alive, we can stop
					log.Printf("[Node %s] Got an answer back from replica: %s", c.id, answer.GetId())
					alive = true
				}
			}(replica, replicaId)
		}

		// Wait for all election calls to finish
		wg.Wait()

		if !alive {
			log.Printf("[Node %s] No replicas responded, I will promote myself to leader!", c.id)
			c.broadcastVictory() //Broadcast to other replicas
			c.mu.Lock()
			c.isLeader = true
			c.mu.Unlock()
			c.server.UpdateLeaderStatus(true)
		} else {
			//A replica is alive and we can let them continue the election process
		}
	}

}

func (c *BackendClient) broadcastVictory() {
	c.mu.Lock()
	replicas := make(map[string]proto.BackendClient, len(c.replicas))
	maps.Copy(replicas, c.replicas)
	*c.clk++ //Update clock once, as we "broadcast" once...
	c.mu.Unlock()

	var wg sync.WaitGroup

	for replicaId, replica := range replicas {
		if replicaId == c.id {
			continue // skip self
		}

		wg.Add(1)
		go func(r proto.BackendClient, rid string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			msg := &proto.Message{
				Id:    c.id,
				Clock: *c.clk}
			_, err := r.Victory(ctx, msg)
			if err != nil {
				log.Printf("[Node %s] Failed to send Victory to replica %s: %v", c.id, rid, err)
			} else {
				log.Printf("[Node %s] Successfully sent Victory to replica %s", c.id, rid)
			}
		}(replica, replicaId)
	}

	wg.Wait()
	log.Printf("[Node %s] Victory broadcast complete", c.id)
}

func (b *BackendServer) Ping(ctx context.Context, empty *emptypb.Empty) (*proto.Answer, error) {
	// Simply return OK
	return &proto.Answer{Clock: int32(b.clk), Id: b.id}, nil
}

func (b *BackendServer) Election(ctx context.Context, msg *proto.Message) (*proto.Answer, error) {
	b.mu.Lock()
	b.clk = max(msg.Clock, b.clk) + 1
	b.mu.Unlock()
	go b.client.callForElection()                            //Start our own election process
	return &proto.Answer{Id: b.id, Clock: int32(b.clk)}, nil //Return ok to node calling for election
}

func (b *BackendServer) Victory(ctx context.Context, msg *proto.Message) (*proto.Ack, error) {
	b.mu.Lock()
	b.clk = max(msg.Clock, b.clk) + 1
	c := b.client
	b.mu.Unlock()
	c.mu.Lock()
	c.leader = c.replicas[msg.GetId()]
	c.leaderId = msg.GetId()
	c.lastLeaderSeen = time.Now()
	c.mu.Unlock()

	log.Printf("[Node %s] Leader updated via election: %s", c.id, msg.GetId())
	return &proto.Ack{Clock: int32(b.clk), Ack: proto.AckTypes_SUCCESS}, nil
}
