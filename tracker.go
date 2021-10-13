package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
)

// TODO: Handle disconnect removals

/*
JSON Unmarshalling:
   To unmarshal JSON into an interface value, Unmarshal stores one of these in the interface value:
   bool, for JSON booleans float64, for JSON numbers string, for JSON strings []interface{}, for JSON arrays map[string]interface{}, for JSON objects nil for JSON null
*/

type Peer struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	ListenPort int `json:"listen_port"`
	// Link back to the chunks it's related to.
	chunks []*Chunk // Loopback
}

func (p Peer) RemoteAddr() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}

type Chunk struct {
	seqId int
	//hash  string // SHA-1 Hash function
	size  uint64 // number of bytes
	peers []*Peer
}

type File struct {
	filename string
	length   uint64 // Size in bytes
	chunks   []Chunk
	sync.RWMutex
}

type Packet struct {
	Type    string                 `json:"t"`
	Subject string                 `json:"s"`
	Message map[string]interface{} `json:"m"`
	peer    *Peer
}

type Tracker struct {
	fileMap map[string]*File
	fileMut *sync.RWMutex
	peerMap map[string]*Peer
	peerMut *sync.RWMutex
}

func copyPeers(peers []*Peer) []Peer {
	resp := make([]Peer, len(peers))

	for i, peer := range peers {
		resp[i] = Peer{
			IP:   peer.IP,
			Port: peer.ListenPort,
		}
	}
	return resp
}

func (t *Tracker) ChunkRegister(p Packet) (resp Packet, toSend bool) {
	filename := p.Message["filename"].(string)
	chunkSeqId := int(p.Message["seqId"].(float64))

	t.fileMut.RLock()
	if actualFile, ok := t.fileMap[filename]; ok {
		t.fileMut.RUnlock()

		chunk := &actualFile.chunks[chunkSeqId]
		actualFile.Lock()
		chunk.peers = append(chunk.peers, p.peer)
		actualFile.Unlock()

		// Add to Peer Loopback
		p.peer.chunks = append(p.peer.chunks, chunk)
	}

	return Packet{"r", "cr", map[string]interface{}{"ack": true}, &Peer{}}, true
}

func (t Tracker) FileLocation(p Packet) (resp Packet, toSend bool) {
	// Serialization Struct
	type ChunkSer struct {
		Peers []Peer `json:"peers"`
		Size uint64 `json:"size"`
	}

	res := map[int]ChunkSer{}
	var fileLen uint64 = 0
	numChunks := 0

	filename := p.Message["filename"].(string)

	t.fileMut.RLock()
	if file, ok := t.fileMap[filename]; ok {
		t.fileMut.RUnlock()

		file.RLock()
		fileLen = file.length
		numChunks = len(file.chunks)

		for _, chunk := range file.chunks {
			res[chunk.seqId] = ChunkSer{
				Peers: copyPeers(chunk.peers),
				Size:  chunk.size,
			}
		}
		file.RUnlock()
	}

	return Packet{"r", "floc", map[string]interface{}{"chunks": res, "length": fileLen, "numChunks": numChunks}, &Peer{}}, true
}

func (t Tracker) FileList(p Packet) (resp Packet, toSend bool) {
	res := map[string]uint64{}

	t.fileMut.RLock()
	for filename, file := range t.fileMap {
		res[filename] = file.length
	}
	t.fileMut.RUnlock()

	return Packet{"r", "fl", map[string]interface{}{"files": res}, &Peer{}}, true
}

func (t *Tracker) RegisterPeer(p Packet) (resp Packet, toSend bool) {
	peer := p.peer
	remoteAddr := peer.RemoteAddr()

	t.peerMut.RLock()
	if _, ok := t.peerMap[remoteAddr]; !ok {
		t.peerMut.RUnlock()

		listenPort, _ := strconv.Atoi(p.Message["listen_port"].(string))
		peer.ListenPort = listenPort
		
		t.peerMut.Lock()
		// TODO: ReRegister?
		t.peerMap[remoteAddr] = peer
		t.peerMut.Unlock()
	} else {
		t.peerMut.RUnlock()
	}

	files := p.Message["files"].(map[string]interface{})
	for filename, lenNoType := range files {
		fileLen := lenNoType.(float64)
		numChunks := int(math.Ceil(fileLen / CHUNK_SIZE_BYTES))

		var actualFile *File
		t.fileMut.RLock()
		if currentFile, ok := t.fileMap[filename]; ok {
			t.fileMut.RUnlock()

			actualFile = currentFile
		} else {
			t.fileMut.RUnlock()

			t.fileMut.Lock()
			actualFile = &File{filename: filename, length: uint64(fileLen), chunks: make([]Chunk, numChunks)}
			t.fileMap[filename] = actualFile
			t.fileMut.Unlock()
		}

		for i := 0; i < numChunks; i++ {
			chunk := &(actualFile.chunks[i])
			chunk.seqId = i
			if i == int(numChunks)-1 {
				// >///<
				chunk.size = uint64(math.Min(float64(uint64(fileLen)%CHUNK_SIZE_BYTES_INT), CHUNK_SIZE_BYTES))
			} else {
				chunk.size = CHUNK_SIZE_BYTES_INT
			}

			if chunk.peers == nil {
				chunk.peers = make([]*Peer, 1)
				chunk.peers[0] = peer
			} else {
				// Current Peer as peer to all chunks
				chunk.peers = append(chunk.peers, peer)
			}

			// Add chunk to peer
			peer.chunks = append(peer.chunks, chunk)
		}
	}

	return Packet{Type: "r", Subject: "reg", Message: map[string]interface{}{"ack": true, "filename": true}}, true
}

var tracker = Tracker{fileMap: make(map[string]*File), peerMap: make(map[string]*Peer), fileMut: &sync.RWMutex{}, peerMut: &sync.RWMutex{}}

var funcMap = map[string]func(Packet) (Packet, bool){
	"reg":  tracker.RegisterPeer,
	"fl":   tracker.FileList,
	"floc": tracker.FileLocation,
	"cr":   tracker.ChunkRegister,
}

func clearPeerChunks(p *Peer) {
	remote := p.RemoteAddr()
	for _, chunk := range p.chunks {
		for i, tP := range chunk.peers {
			if tP.RemoteAddr() == remote {
				n := len(chunk.peers)
				chunk.peers[i] = chunk.peers[n - 1] // Replace into removed position
				chunk.peers = chunk.peers[:n-1] // Reslice to make it smaller
			}
		}
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()
	remoteAddr := c.RemoteAddr().String()
	splits := strings.Split(remoteAddr, ":")

	var currentPeer *Peer
	if peer, ok := tracker.peerMap[remoteAddr]; ok {
		currentPeer = peer
	} else {
		port, e := strconv.Atoi(splits[1])
		if e != nil {
			return
		}
		currentPeer = &Peer{
			IP:   splits[0],
			Port: port,
			chunks: make([]*Chunk, 0),
		}
	}
	fmt.Printf(" [debug] Accepted Connection from %s\n", remoteAddr)
	for {
		reader := bufio.NewReader(c)
		netData, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[debug] Disconnected from peer %s with reason: %s\n", remoteAddr, err)
			clearPeerChunks(currentPeer)
			return
		}

		temp := strings.TrimSpace(netData)
		header := strings.Split(temp, "=")
		if header[0] != "LENGTH" {
			continue
		}

		jsonByteLength, err := strconv.Atoi(header[1])
		if err != nil {
			fmt.Printf("[error][%s] At reading length: %s\n", remoteAddr, err)
			return
		}

		buf := make([]byte, jsonByteLength)
		n, err := io.ReadFull(reader, buf)
		if err != nil || n < jsonByteLength {
			fmt.Printf("[error][%s] Reading %d json bytes : %s\n", remoteAddr, jsonByteLength, err)
			return
		}
		var p Packet
		err = json.Unmarshal(buf, &p)
		if err != nil {
			fmt.Printf("[error][%s] Parsing json: %s\n", remoteAddr, jsonByteLength, err)
			return
		}
		p.peer = currentPeer
		fmt.Printf("[info][%s] Got Packet: %s\n", remoteAddr, p)

		if p.Type == "q" && p.Subject == "stop" {
			// Deferred close is enabled.
			return
		}

		if pFunc, ok := funcMap[p.Subject]; ok {
			resp, toSend := pFunc(p)
			if toSend {
				b, _ := json.Marshal(resp)
				fmt.Fprintf(c, "LENGTH=%d\n", len(b))
				_, err = c.Write(b)
				if err != nil {
					fmt.Printf("[error][%s] Error Writing to Conn: %s\n", remoteAddr, err)
					return
				}
			}
		}
	}
}

var (
	PORT                 = "8080"
	CHUNK_SIZE_BYTES     = 1e6
	CHUNK_SIZE_BYTES_INT = uint64(CHUNK_SIZE_BYTES)
)

func main() {
	l, err := net.Listen("tcp4", "0.0.0.0:"+PORT)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	fmt.Println("Server started listening on", PORT)

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c)
	}
}

func main2() {
	p := Packet{"r", "flist", map[string]interface{}{"test": 1}, &Peer{}}
	fmt.Println(p)
	b, _ := json.Marshal(p)
	fmt.Println(string(b))

	type ChunkSer struct {
		Peers []Peer `json:"peers"`
		Size uint64 `json:"size"`
	}

	b, _ = json.Marshal(map[string]interface{}{
			"chunks": map[int]interface{}{
				1: ChunkSer{
					Peers: []Peer{Peer{
						IP:   "127.0.0.1",
						Port: 23234,
					}},
					Size: 1234,
				},
			},
		})
	fmt.Println(string(b))
	s := map[string]interface{}{}
	json.Unmarshal(b, &s)
	fmt.Println(((s["chunks"].(map[string]interface{}))["1"].(map[string]interface{}))["peers"].([]interface{})[0])

	o := base64.StdEncoding.EncodeToString(b)
	fmt.Println(o)

	b, _ = base64.StdEncoding.DecodeString(o)
	fmt.Println(string(b))
}
