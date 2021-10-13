# CSE 513 - Project 1 - Peer to Peer File Sharing System

## Overview

This system allows one to share files across different machines, by means of a peer to peer network of nodes.

## Requirements

We only require `golang-v1.17` . No external dependencies are added.

## Repository Structure

The repository consists of two files:

 - `peer.go`: Contains the codebase for the peer.
 - `tracker.go`: Server side code base.
 - `Makefile`: Makefile used for building the application

## Building

The application can be compiled into a binary by running the following commands:

```sh
> make
mkdir -p build
go build -o build/peer peer.go
go build -o build/server tracker.go
```

## Execution

### Server
You need to run `build/server` to start the server.

Sample output:

```sh
> ./build/server
Server started listening on 8080
...
```

### Peer
On the peer host run the binary from `build/peer`

```sh
> ./build/peer
Hello User! Welcome back to get your files


 [debug] Got file folder: .
 [debug] Started listening on port 0.0.0.0:36039
fs>
```
