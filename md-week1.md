# Instructions for Claude Code - Week 1: Skeleton and RPC Contracts

## Task Overview
You are tasked with bootstrapping the codebase for **Project B5: Replicated, Strongly Consistent Key-Value Store in Go** according to the project technical specification[cite: 1].

The goal for Week 1 is to set up the Go module repository structure, write and generate all Protocol Buffer contracts, create basic executable stubs for all 4 microservices, integrate required core libraries, and provide a valid `docker-compose.yml` to verify Docker network connectivity[cite: 1].

---

## 1. Project Directory & Module Setup

Initialize a single Go module (`go.mod`) named `b5-kvstore` and implement the standard layout required by §13 of the specification[cite: 1]:

```text
b5-kvstore/
├── api/
│   └── proto/                  # Protocol Buffer source files (.proto)
├── pkg/
│   └── pb/                     # Generated Go code from Protobuf
├── cmd/
│   ├── consensus-node/         # Main entrypoint for consensus node
│   ├── client-proxy/           # Main entrypoint for client proxy
│   ├── snapshot-backup/        # Main entrypoint for snapshot & backup service
│   └── discovery/              # Main entrypoint for service discovery
├── internal/
│   ├── raft/                   # Core consensus and log logic
│   ├── statemachine/           # Local Key-Value store state machine
│   ├── proxy/                  # HTTP REST to gRPC translation & routing
│   ├── discovery/              # Node discovery registry & health checking
│   ├── circuitbreaker/         # Circuit breaker client wrappers
│   └── snapshot/               # Log compaction & snapshot management
├── deployments/
│   ├── Dockerfile.consensus
│   ├── Dockerfile.proxy
│   ├── Dockerfile.snapshot
│   ├── Dockerfile.discovery
│   └── docker-compose.yml
├── go.mod
├── go.sum
├── Makefile
└── README.md