# Running the Auction server & Clients
Zeroconf installation for mDNS discovery of nodes on a local network As this project depends on the package zeroconf, installation is required: go get -u github.com/grandcat/zeroconf

To run multiple clients & servers use different terminals to run each of them. As long as they are on a local network, the mDNS discovery will work.

To start a client use the command:

`go run client/client.go <username> <clientId>`

ex.

`go run client/client.go Alice 1`

To start a server use the command:
`go run server/server.go <frontendPort> <nodeId> <backendPort> <isLeader>`

to, for example, start 2 servers do:

Server 1: `go run server/server.go 5001 1 5011 1`

Server 2: `go run server/server.go 5002 2 5012 0`

```text
Project structure
/ (repo)
├─ grpc/        # grpc .proto definition
├─ logs/        # logs 
├─ server/server.go      # Auction server code
├─ client/client.go      # Client code
├─ go.mod       # Go module
├─ go.sum       # Go module
└─ readme.md    # This readme.md
```