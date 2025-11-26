package main

import (
	"context"
	"fmt"
	"log"
	proto "main/grpc"
	"os"
	"strconv"
	"time"

	"github.com/grandcat/zeroconf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type AuctionClient struct {
	client   proto.AuctionClient
	conn     *grpc.ClientConn
	disc     chan AuctionNodeInfo
	username string
	id       int32
	clk      int32
}

type AuctionNodeInfo struct {
	nodeId  string
	address string
}

func main() {

	aClient := &AuctionClient{clk: 0}
	aClient.start_client()

}

func (c *AuctionClient) start_client() {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	//Default values
	username := "John"

	c.clk = 0 //Lamport Clock

	if len(os.Args) > 1 {
		username = os.Args[1] //Override default username
	}
	if len(os.Args) > 2 {
		id, _ := strconv.Atoi(os.Args[2]) //client id
		c.id = int32(id)
	}
	c.username = username
	//Start up and configure logging output to file
	f, err := os.OpenFile("logs/client"+c.username+"log"+time.Now().Format("20060102150405")+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	//defer to close when we are done with it.
	defer f.Close()

	//set output of logs to f
	log.SetOutput(f)
	c.disc = make(chan AuctionNodeInfo)
	go c.discoverAuctionNodes(c.disc)

	c.connectNextNode()

	isOver := false
	currentBid := 0 //Currently accepted bid at the auction server
	nextBid := 1    //Start at 1
	winning := false

	for !isOver {
		for winning {
			c.clk++
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			result, err := c.client.Result(ctx, &emptypb.Empty{})
			cancel()
			if err != nil {
				log.Printf("[Client %s] failed %v", c.username, err.Error())
				c.conn.Close()
				c.connectNextNode()
				continue
			} else if result.IsOver {
				break
			} else if result.Result > int32(currentBid) {
				winning = false
				nextBid = int(result.Result) + 1
				break
			}
			time.Sleep(1 * time.Second)
		}
		c.clk++
		log.Printf("[Client %s] Trying to bid... (%d)", c.username, nextBid)
		res, err := c.client.Bid(context.Background(), &proto.Amount{Clock: c.clk, Amount: int32(nextBid), ClientId: fmt.Sprintf("%d", c.id)})
		if err != nil {
			log.Printf("[Client %s] failed to bid! %v", c.username, err.Error())
			c.conn.Close()
			c.connectNextNode()
			continue
		}
		c.clk = max(res.Clock, c.clk) + 1
		switch res.Ack {
		case proto.AckTypes_SUCCESS:
			currentBid = nextBid
			log.Printf("[Client %s] Bid successful", c.username)
			winning = true
		case proto.AckTypes_FAIL:
			log.Printf("[Client %s] Bid rejected", c.username)
		default:
			log.Printf("[Client %s] Exception happened when trying to bid", c.username)
		}
		c.clk++
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		result, err := c.client.Result(ctx, &emptypb.Empty{})
		cancel()
		if err != nil {
			log.Printf("[Client %s] failed %v", c.username, err.Error())
			c.conn.Close()
			c.connectNextNode()
		}
		isOver = result.IsOver
		if result.Result > int32(currentBid) {
			nextBid = int(result.Result) + 1
			winning = false
		} else if winning && isOver {
			log.Printf("[Client %s] Won the auction! with the bid %d, at Clock %d", c.username, currentBid, result.Clock)
			break
		} else if isOver {
			log.Printf("[Client %s] Lost the auction! with the bid %d, at Clock %d", c.username, currentBid, result.Clock)
			break
		}
		time.Sleep(1 * time.Second)
		defer c.conn.Close()
	}
}

func (c *AuctionClient) connectNextNode() {
	if c.conn != nil {
		c.conn.Close()
	}

	for {
		auctionNode := <-c.disc // next discovered server
		opts := grpc.WithTransportCredentials(insecure.NewCredentials())
		conn, err := grpc.NewClient(auctionNode.address, opts)
		c.conn = conn
		c.client = proto.NewAuctionClient(conn)
		if err != nil {
			log.Printf("[Client %s] failed to connect to %s: %v",
				c.username, auctionNode.address, err)
			continue // try the next one
		} else {
			log.Printf("[Client %s] connected successfully!", c.username)
		}

		log.Printf("[Client %s] connected to %s", c.username, auctionNode.address)
		return
	}
}

func (c *AuctionClient) discoverAuctionNodes(discovered chan<- AuctionNodeInfo) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		log.Fatalf("[Client %s] Failed to initialize resolver: %v", c.username, err)
	}

	entries := make(chan *zeroconf.ServiceEntry)

	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {

			addr := fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), entry.Port)

			peerID := entry.Instance[len("node-"):]

			log.Printf("[Client %s] Discovered server node: %s (%s)", c.username, peerID, addr)
			discovered <- AuctionNodeInfo{nodeId: peerID, address: addr}
		}
	}(entries)

	// Long-lived browse context
	// Start browsing
	ctx := context.Background()
	go func() {
		for {
			if err := resolver.Browse(ctx, "_auctionFrontend._tcp", "local.", entries); err != nil {
				log.Printf("[Client %s] Browse error: %v", c.username, err)
			}
			time.Sleep(5 * time.Second) // Retry interval
		}
	}()
}
