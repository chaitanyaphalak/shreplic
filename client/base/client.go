package base

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vonaka/shreplic/master/defs"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
	"github.com/vonaka/shreplic/tools/fastrpc"
)

type Client struct {
	N             int
	Seqnum        int32
	ClientId      int32
	LeaderId      int
	ClosestId     int
	LastPropose   smr.Propose
	CollocatedId  int
	LastSubmitter int

	Ping       []float64
	MinLatency float64
	MaxLatency float64

	Fast       bool
	Verbose    bool
	LocalRead  bool
	Leaderless bool

	RPC       *fastrpc.Table
	ResChan   chan []byte
	Waiting   chan struct{}
	ReadTable bool

	servers []net.Conn
	readers []*bufio.Reader
	writers []*bufio.Writer

	Logger         *log.Logger
	masterPort     int
	masterAddr     string
	replicaList    []string
	collocatedWith string
}

const TIMEOUT = 3 * time.Second

func NewClient(maddr string, mport int, fast, lread, leaderLess, verbose bool) *Client {
	return NewClientWithLog(maddr, mport, fast, lread, leaderLess, verbose, nil)
}

func NewClientWithLog(maddr string, mport int,
	fast, lread, leaderless, verbose bool, logger *log.Logger) *Client {

	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	return &Client{
		N:             0,
		Seqnum:        -1,
		ClientId:      newId(),
		LeaderId:      -1,
		ClosestId:     -1,
		CollocatedId:  -1,
		LastSubmitter: -1,

		MinLatency: 0.0,
		MaxLatency: 0.0,

		Fast:       fast,
		Verbose:    verbose,
		LocalRead:  lread,
		Leaderless: leaderless,

		RPC:       fastrpc.NewTableId(smr.RPC_TABLE),
		ResChan:   make(chan []byte, 8),
		Waiting:   make(chan struct{}, 8),
		ReadTable: false,

		servers: nil,
		readers: nil,
		writers: nil,

		Ping: []float64{},

		Logger:         logger,
		masterPort:     mport,
		masterAddr:     maddr,
		replicaList:    nil,
		collocatedWith: "",
	}
}

func (c *Client) Collocated(with string) {
	c.collocatedWith = with
}

func (c *Client) Connect() error {
	c.Println("Dialing master...")
	master, err := c.dialMaster()
	if err != nil {
		return err
	}
	defer master.Close()

	c.Println("Getting list of replicas...")
	rl, err := askMaster(master, "GetReplicaList", c.Logger)
	if err != nil {
		return err
	}
	masterReply := rl.(*defs.GetReplicaListReply)
	c.replicaList = masterReply.ReplicaList

	c.Println("Searching for the closest replica...")
	err = c.findClosestReplica(masterReply.AliveList)
	if err != nil {
		return err
	}
	c.Println("Node list", c.replicaList)
	c.Println("Closest (alive)", c.ClosestId)

	c.N = len(c.replicaList)
	c.servers = make([]net.Conn, c.N)
	c.readers = make([]*bufio.Reader, c.N)
	c.writers = make([]*bufio.Writer, c.N)

	if !c.Leaderless {
		c.Println("Getting leader from master...")
		gl, err := askMaster(master, "GetLeader", c.Logger)
		if err != nil {
			return err
		}
		masterReply := gl.(*defs.GetLeaderReply)
		c.LeaderId = masterReply.LeaderId
		c.Println("The leader is replicas", c.LeaderId)
	}

	toConnect := []int{}
	if c.Fast {
		for i := 0; i < c.N; i++ {
			if masterReply.AliveList[i] {
				toConnect = append(toConnect, i)
			}
		}
	} else {
		toConnect = append(toConnect, c.ClosestId)
		if !c.Leaderless && c.ClosestId != c.LeaderId {
			toConnect = append(toConnect, c.LeaderId)
		}
	}

	for _, i := range toConnect {
		c.Println("Connection to", i, "->", c.replicaList[i])
		c.servers[i], err = dial(c.replicaList[i], false, c.Logger)
		if err != nil {
			return err
		}
		c.readers[i] = bufio.NewReader(c.servers[i])
		c.writers[i] = bufio.NewWriter(c.servers[i])
		go func(reader *bufio.Reader) {
			// track RPC-table
			for c.ReadTable {
				var (
					msgType uint8
					err     error
				)
				if msgType, err = reader.ReadByte(); err != nil {
					break
				}
				p, exists := c.RPC.Get(msgType)
				if !exists {
					c.Println("Error: received unknown message:", msgType)
					continue
				}
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				go func() {
					p.Chan <- obj
				}()
			}
		}(c.readers[i])
	}

	c.Println("Connected")
	return nil
}

func (c *Client) Disconnect() {
	for _, server := range c.servers {
		if server != nil {
			server.Close()
		}
	}
	c.Println("Disconnected")
}

func (c *Client) Write(key state.Key, value []byte) {
	c.Seqnum++
	args := smr.Propose{
		CommandId: c.Seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.PUT,
			K:  key,
			V:  value,
		},
		Timestamp: 0,
	}

	c.Println(args.Command.String())
	c.execute(args)
}

func (c *Client) Read(key state.Key) []byte {
	c.Seqnum++
	args := smr.Propose{
		CommandId: c.Seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.GET,
			K:  key,
			V:  state.NIL(),
		},
		Timestamp: 0,
	}

	c.Println(args.Command.String())
	return c.execute(args)
}

func (c *Client) Scan(key state.Key, count int64) []byte {
	c.Seqnum++
	args := smr.Propose{
		CommandId: c.Seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.SCAN,
			K:  key,
			V:  make([]byte, 8)},
		Timestamp: 0,
	}

	binary.LittleEndian.PutUint64(args.Command.V, uint64(count))

	c.Println(args.Command.String())
	return c.execute(args)
}

func (c *Client) Stats() string {
	c.writers[c.ClosestId].WriteByte(smr.STATS)
	c.writers[c.ClosestId].Flush()
	arr := make([]byte, 1000)
	c.readers[c.ClosestId].Read(arr)
	return string(bytes.Trim(arr, "\x00"))
}

func (c *Client) ProposeReplyFrom(rid int) (*smr.ProposeReplyTS, error) {
	rep := &smr.ProposeReplyTS{}
	err := rep.Unmarshal(c.readers[rid])
	return rep, err
}

func (c *Client) Println(v ...interface{}) {
	if c.Verbose {
		c.Logger.Println(v...)
	}
}

func (c *Client) Printf(format string, v ...interface{}) {
	if c.Verbose {
		c.Logger.Printf(format, v...)
	}
}

func (c *Client) SendMsg(rid int32, code uint8, msg fastrpc.Serializable) {
	w := c.writers[rid]
	if w == nil {
		log.Printf("%d: no associated writer", rid)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (c *Client) execute(args smr.Propose) []byte {
	submitter := c.LeaderId
	if c.Leaderless {
		submitter = c.ClosestId
	}
	c.LastSubmitter = submitter
	c.LastPropose = args

	if !c.Fast {
		c.Println("Sent to", submitter)
		c.writers[submitter].WriteByte(smr.PROPOSE)
		args.Marshal(c.writers[submitter])
		c.writers[submitter].Flush()
	} else {
		c.Println("Sent to everyone", args.CommandId)
		for rep := 0; rep < c.N; rep++ {
			if c.writers[rep] != nil {
				c.writers[rep].WriteByte(smr.PROPOSE)
				args.Marshal(c.writers[rep])
				c.writers[rep].Flush()
			}
		}
	}

	c.Waiting <- struct{}{}
	return <-c.ResChan
}

func (c *Client) findClosestReplica(alive []bool) error {
	c.Logger.Println("Pinging all replicas...")

	found := false
	minLatency := math.MaxFloat64
	maxLatency := 0.0
	for i := 0; i < len(c.replicaList); i++ {
		if !alive[i] {
			continue
		}
		addr := strings.Split(string(c.replicaList[i]), ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}

		if addr == c.collocatedWith {
			found = true
			c.CollocatedId = i
			c.ClosestId = i
		}

		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			c.Logger.Println(i, "->", latency)
			c.Ping = append(c.Ping, latency)

			if minLatency > latency {
				if !found {
					c.ClosestId = i
				}
				minLatency = latency
			}
			if maxLatency < latency {
				maxLatency = latency
			}
		} else {
			c.Logger.Println("Cannot ping", c.replicaList[i])
			return err
		}
	}
	c.MinLatency = minLatency
	c.MaxLatency = maxLatency

	return nil
}

func (c *Client) dialMaster() (*rpc.Client, error) {
	addr := fmt.Sprintf("%s:%d", c.masterAddr, c.masterPort)
	conn, err := dial(addr, true, c.Logger)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func dial(addr string, connect bool, logger *log.Logger) (net.Conn, error) {
	var (
		err  error    = nil
		conn net.Conn = nil
		resp *http.Response
	)

	for try := 0; try < 3; try++ {
		conn, err = net.DialTimeout("tcp", addr, TIMEOUT)
		if err == nil {
			if connect {
				io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
				resp, err = http.ReadResponse(bufio.NewReader(conn),
					&http.Request{
						Method: "CONNECT",
					})
				if err == nil && resp != nil && resp.Status == "200 Connected to Go RPC" {
					return conn, nil
				}
			} else {
				return conn, nil
			}
		} else {
			logger.Println(addr, "connection error:", err)
		}
		if conn != nil {
			conn.Close()
		}
	}

	return nil, errors.New("cannot connect")
}

func askMaster(master *rpc.Client, method string, l *log.Logger) (interface{}, error) {
	var (
		gl     *defs.GetLeaderReply
		rl     *defs.GetReplicaListReply
		glArgs *defs.GetLeaderArgs
		rlArgs *defs.GetReplicaListArgs
	)

	for i := 0; i < 5; i++ {
		if method == "GetReplicaList" {
			rl = &defs.GetReplicaListReply{}
			rlArgs = &defs.GetReplicaListArgs{}
			err := call(master, "Master."+method, rlArgs, rl, l)
			if err == nil && rl.Ready {
				return rl, nil
			}
		} else if method == "GetLeader" {
			gl = &defs.GetLeaderReply{}
			glArgs = &defs.GetLeaderArgs{}
			err := call(master, "Master."+method, glArgs, gl, l)
			if err == nil {
				return gl, nil
			}
		}
	}

	return nil, errors.New("Too many call attempts!")
}

func call(c *rpc.Client, method string, args, reply interface{}, l *log.Logger) error {
	errs := make(chan error, 1)
	go func() {
		errs <- c.Call(method, args, reply)
	}()
	select {
	case err := <-errs:
		if err != nil {
			l.Println("Error in RPC: " + method)
		}
		return err

	case <-time.After(TIMEOUT):
		l.Println("RPC timeout: " + method)
		return errors.New("RPC timeout")
	}
}

func newId() int32 {
	return int32(uuid.New().ID())
}
