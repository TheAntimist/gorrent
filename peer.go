package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

// Persistent Peer Connection until disconnect.

type Peer struct {
	IP   string
	Port int
	reqChan chan *Chunk
}

func (p Peer) RemoteAddr() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}

type Chunk struct {
	seqId int
	//hash  string // SHA-1 Hash function
	size  uint64 // number of bytes
	completed bool
	peers []*Peer // Required?
	file *File // Back link
}

func (chunk Chunk) fileOffset() int64 {
	return int64(chunk.seqId)*int64(CHUNK_SIZE_BYTES_INT)
}

type File struct {
	filename string
	length   uint64 // Size in bytes
	chunks   []*Chunk
	completed bool
	sync.RWMutex
}

func (f *File) location(folder string) string {
	return path.Join(folder, f.filename)
}

type Packet struct {
	Type    string                 `json:"t"`
	Subject string                 `json:"s"`
	Message map[string]interface{} `json:"m"`
}

var (
	CHUNK_SIZE_BYTES     = 1e6
	CHUNK_SIZE_BYTES_INT = uint64(CHUNK_SIZE_BYTES)
)

type Client struct {
	conn net.Conn
	listenPort string
	FOLDER string
	fileMap map[string]*File
	fileMut *sync.RWMutex
}

var client = Client{
	conn:   nil,
	FOLDER: ".",
	fileMut: &sync.RWMutex{},
	fileMap: map[string]*File{},
}

func (c *Client) init() {
	files, _ := ioutil.ReadDir(c.FOLDER)
	c.fileMut.Lock()
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileLen := uint64(file.Size())

		numChunks := int(math.Ceil(float64(file.Size()) / CHUNK_SIZE_BYTES))
		fileObj := &File{filename: file.Name(), length: fileLen, chunks: make([]*Chunk, 0, numChunks), completed: true}

		for i := 0; i < numChunks; i++ {
			chunk := &Chunk{
				seqId: i,
				size:  0,
				file:  fileObj,
				completed: true,
			}
			if i == int(numChunks)-1 {
				// >///<
				chunk.size = uint64(math.Min(float64(fileLen%CHUNK_SIZE_BYTES_INT), CHUNK_SIZE_BYTES))
			} else {
				chunk.size = CHUNK_SIZE_BYTES_INT
			}
			fileObj.chunks = append(fileObj.chunks, chunk)
		}

		c.fileMap[file.Name()] = fileObj
	}
	c.fileMut.Unlock()

	c.listen()
}

func handleResponse(conn net.Conn) (Packet, error) {
	remoteAddr := conn.RemoteAddr()
	reader := bufio.NewReader(conn)
	headerData, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("[error][%s] Error reading from conn: %s\n", remoteAddr, err)
		return Packet{}, err
	}

	header := strings.Split(strings.TrimSpace(headerData), "=")
	if header[0] != "LENGTH" {
		return Packet{}, err
	}

	jsonByteLength, err := strconv.Atoi(header[1])
	if err != nil {
		fmt.Printf("[error][%s] At reading length: %s\n", remoteAddr, err)
		return Packet{}, err
	}

	buf := make([]byte, jsonByteLength)
	n, err := io.ReadFull(reader, buf)
	if err != nil || n < jsonByteLength {
		fmt.Printf("[error][%s] Reading %d json bytes : %s\n", remoteAddr, jsonByteLength, err)
		return Packet{}, err
	}
	var p Packet
	err = json.Unmarshal(buf, &p)
	if err != nil {
		fmt.Printf("[error][%s] Parsing json: %s\n", remoteAddr, jsonByteLength, err)
		return Packet{}, err
	}

	return p, nil
}

func (c Client) handleResponse() (Packet, error) {
	return handleResponse(c.conn)
}

func sendRequest(conn net.Conn, p Packet) error {
	b, _ := json.Marshal(p)
	fmt.Fprintf(conn, "LENGTH=%d\n", len(b))
	_, err := conn.Write(b)
	if err != nil {
		fmt.Printf("[error][%s] Error Writing to Conn: %s\n", conn.RemoteAddr(), err)
		return err
	}
	return nil
}

func (c Client) sendRequest(p Packet) error {
	return sendRequest(c.conn, p)
}

func (c Client) getCurrentFiles() map[string]uint64 {
	res := make(map[string]uint64, len(c.fileMap))
	c.fileMut.RLock()
	for name, file := range c.fileMap {
		res[name] = file.length
	}
	c.fileMut.RUnlock()
	return res
}

func (c *Client) start(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: start server_ip port")
		return
	}
	remoteAddr := strings.Join(args[:2], ":")
	c.conn, _ = net.Dial("tcp", remoteAddr)

	//if err != nil {
	//	fmt.Printf(" [error] Connection failed to server %s\n", remoteAddr)
	//}

	files := c.getCurrentFiles()
	c.sendRequest(Packet{"q", "reg", map[string]interface{}{
		"files": files,
		"listen_port": c.listenPort,
	}})
	c.handleResponse()
}


func byteCountIEC(b float64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%.1f B", b)
	}
	div, exp := float64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		b/div, "KMGTPE"[exp])
}

func (c *Client) listFiles(args []string) {
	c.sendRequest(Packet{
		Type:    "q",
		Subject: "fl",
		Message: nil,
	})
	p, _ := c.handleResponse()
	data := make([][]string, 0)

	for filename, lenNoType := range p.Message["files"].(map[string]interface{}) {
		data = append(data, []string{filename, byteCountIEC(lenNoType.(float64))})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Length"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func (c *Client) locateFile(args []string) {
	filename := strings.Join(args, " ")
	c.sendRequest(Packet{
		Type:    "q",
		Subject: "floc",
		Message: map[string]interface{}{
			"filename": filename,
		},
	})
	p, _ := c.handleResponse()
	fmt.Printf("For file %s we have the following details\n", filename)
	data := make([][]string, 0)

	for chunkId, chunkGen := range p.Message["chunks"].(map[string]interface{}) {
		builder := strings.Builder{}
		chunk := chunkGen.(map[string]interface{})
		for _, peer := range chunk["peers"].([]interface{}) {
			ac := peer.(map[string]interface{})
			builder.WriteString(ac["ip"].(string))
			builder.WriteString(":")
			builder.WriteString(strconv.Itoa(int(ac["port"].(float64))))
			builder.WriteString(" ")
		}
		data = append(data, []string{chunkId, builder.String()})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Chunk ID", "Peers"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func (c *Client) safeExit(args []string) {
	// TODO: Fix this
	if c.conn != nil {
		c.conn.Close()
	}
	os.Exit(0)
}

func (c Client) hello(args []string) {
	fmt.Println("Hello! Args:", strings.Join(args, " "))
}

func (c Client) fileChunkRequest(p Packet) (Packet, bool) {
	chunkId := int(p.Message["cid"].(float64))
	filename := p.Message["filename"].(string)

	c.fileMut.RLock()
	if file, ok := c.fileMap[filename]; ok {
		// TODO: needed?
		file.RLock()
		chunk := file.chunks[chunkId]
		file.RUnlock()

		f, err := os.Open(file.location(c.FOLDER))
		defer f.Close()

		if err != nil {
			fmt.Printf(" [error] Error opening file %s\n", file.location(c.FOLDER))
			return Packet{}, false
		}
		f.Seek(chunk.fileOffset(), io.SeekStart)

		buffer := make([]byte, chunk.size)
		_, err = io.ReadFull(f, buffer)
		if err != nil {
			fmt.Printf(" [error] Reading %d bytes from file %s\n", chunk.size, file.location(c.FOLDER))
			return Packet{}, false
		}

		outString := base64.StdEncoding.EncodeToString(buffer)

		fmt.Printf("[info][%s] Uploading Chunk %d of size %s \n",
			filename, chunk.seqId, byteCountIEC(float64(chunk.size)))

		return Packet{
			Type: "r",
			Subject: "fc",
			Message: map[string]interface{}{
				"b": outString,
			},
		}, true

	}

	return Packet{}, false
}

var packetFuncMap = map[string]func(Packet) (Packet, bool) {
	"fc": client.fileChunkRequest,
}

func (c *Client) handleConnection(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()

	fmt.Printf(" [debug] Accepted Connection from %s\n", remoteAddr)
	for {
		reader := bufio.NewReader(conn)
		netData, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("[debug] Disconnected from peer %s with reason: %s\n", remoteAddr, err)
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

		//p.peer = currentPeer
		//fmt.Printf("[info][%s] Got Packet: %s\n", remoteAddr, p)

		if p.Type == "q" && p.Subject == "stop" {
			// Deferred close is enabled.
			return
		}

		if pFunc, ok := packetFuncMap[p.Subject]; ok {
			resp, toSend := pFunc(p)
			if toSend {
				b, _ := json.Marshal(resp)
				fmt.Fprintf(conn, "LENGTH=%d\n", len(b))
				_, err = conn.Write(b)
				if err != nil {
					fmt.Printf("[error][%s] Error Writing to Conn: %s\n", remoteAddr, err)
					return
				}
			}
		}
	}
}

func (c *Client) listen() {
	l, err := net.Listen("tcp4", "0.0.0.0:0")

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(" [debug] Started listening on port", l.Addr().String())
	splits := strings.Split(l.Addr().String(), ":")
	c.listenPort = splits[1]

	go func(l net.Listener) {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				return
			}
			go c.handleConnection(conn)
		}
	}(l)
}

func (c *Client) handleDownloadPeer(peer *Peer, fd *os.File, ackChan chan bool) {
	remoteAddr := peer.RemoteAddr()

	conn, _ := net.Dial("tcp", remoteAddr)
	defer conn.Close()

	// TODO: Ack?

	for chunk := range peer.reqChan {
		sendRequest(conn, Packet{
			Type:    "q",
			Subject: "fc",
			Message: map[string]interface{}{
				"cid": chunk.seqId,
				"filename": chunk.file.filename,
			},
		})
		p, _ := handleResponse(conn)

		b64string := p.Message["b"].(string)
		byteBuf, _ := base64.StdEncoding.DecodeString(b64string)
		chunk.file.Lock()
		// Atomically write and register at server
		fd.WriteAt(byteBuf, chunk.fileOffset())
		chunk.completed = true

		c.sendRequest(Packet{
			Type: "q",
			Subject: "cr",
			Message: map[string]interface{}{
				"filename": chunk.file.filename,
				"seqId": chunk.seqId,
			},
		})
		c.handleResponse()
		chunk.file.Unlock()

		fmt.Printf(" [debug][%s] Chunk %d downloaded of size %s\n", chunk.file.filename, chunk.seqId, byteCountIEC(float64(chunk.size)))

		ackChan <- true
	}
}

func (c *Client) handleDownload(f *File, peerMap map[string]*Peer) {
	totalChunks := len(f.chunks)
	ackChan := make(chan bool, totalChunks)

	fd, _ := os.Create(f.location(c.FOLDER))
	defer fd.Close()
	// 0 Padding empty file before writing
	fd.Truncate(int64(f.length))
	peers := make([]*Peer, 0, len(peerMap))

	for _, peer := range peerMap {
		peers = append(peers, peer)
		go c.handleDownloadPeer(peer, fd, ackChan)
	}

	// TODO: Rarest first
	for _, chunk := range f.chunks {
		peers := chunk.peers
		var (
			minPeer = peers[0]
			minLen  = len(peers[0].reqChan)
		)

		for _, peer := range peers {
			if len(peer.reqChan) < minLen {
				minPeer = peer
				minLen = len(peer.reqChan)
			}
		}

		minPeer.reqChan <- chunk
	}

	remainingChunks := totalChunks

	for range ackChan {
		remainingChunks -= 1
		if remainingChunks == 0 {
			break
		}
	}

	close(ackChan)
	for _, p := range peers {
		close(p.reqChan)
	}

	fmt.Printf(" [debug] Downloaded %s of size %s\n", f.filename, byteCountIEC(float64(f.length)))
}

func (c *Client) download(args []string) {
	filename := strings.Join(args, " ")

	c.sendRequest(Packet{
		Type: "q",
		Subject: "floc",
		Message: map[string]interface{}{
			"filename": filename,
		},
	})

	p, _ := c.handleResponse()
	peers := make(map[string]*Peer, 0)
	numChunks := int(p.Message["numChunks"].(float64))

	file := File{
		filename:  filename,
		length: uint64(p.Message["length"].(float64)),
		chunks: make([]*Chunk, numChunks),
		completed: false,
	}

	for chunkId, chunkGen := range p.Message["chunks"].(map[string]interface{}) {
		idx, _ := strconv.Atoi(chunkId)
		chunkMap := chunkGen.(map[string]interface{})
		peerLst := chunkMap["peers"].([]interface{})

		chunk := &Chunk{
			seqId:     idx,
			size:      uint64(chunkMap["size"].(float64)),
			completed: false,
			peers: make([]*Peer, 0, len(peerLst)),
			file: &file,
		}

		for _, peerGen := range peerLst {
			peerMap := peerGen.(map[string]interface{})

			peerObj := Peer{
				IP: peerMap["ip"].(string),
				Port: int(peerMap["port"].(float64)),
				reqChan: make(chan *Chunk, numChunks),
			}
			remoteAddr := peerObj.RemoteAddr()
			if actual, ok := peers[remoteAddr]; !ok {
				peers[remoteAddr] = &peerObj
				chunk.peers = append(chunk.peers, &peerObj)
			} else {
				chunk.peers = append(chunk.peers, actual)
			}
		}
		file.chunks[idx] = chunk
	}

	c.fileMut.Lock()
	c.fileMap[file.filename] = &file
	c.fileMut.Unlock()

	c.handleDownload(&file, peers)
}

func (c Client) printFolder(args []string) {
	fmt.Println("Files are present in folder:", c.FOLDER)
}

var commandMap = map[string]func([]string) {
	"start": client.start,
	"exit": client.safeExit,
	"ls": client.listFiles,
	"list": client.listFiles,
	"locate": client.locateFile,
	"download": client.download,
	"hello": client.hello,
	"folder": client.printFolder,
}

func inputLoop() {
	// create new reader from stdin
	reader := bufio.NewReader(os.Stdin)
	// start infinite loop to continuously listen to input
	for {
		fmt.Print("fs> ")
		// read by one line (enter pressed)
		s, err := reader.ReadString('\n')
		// check for errors
		if err != nil {
			// close channel just to inform others
			return
		}
		s = strings.TrimSpace(s)
		sp := strings.Split(s, " ")
		if len(s) > 0 {
			if commandFunc, ok := commandMap[sp[0]]; ok {
				commandFunc(sp[1:])
			}
		}
	}
}

func main() {
	fmt.Println("Hello User! Welcome back to get your files\n\n")
	if len(os.Args) > 1 {
		client.FOLDER = path.Dir(strings.Join(os.Args[1:], " "))
	}
	fmt.Println(" [debug ]Got file folder:", client.FOLDER)
	client.init()
	inputLoop()
}