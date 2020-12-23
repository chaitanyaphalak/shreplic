package smr

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vonaka/shreplic/state"
	"github.com/vonaka/shreplic/tools/dlog"
	"github.com/vonaka/shreplic/tools/fastrpc"
)

type GPropose struct {
	*Propose
	Reply      *bufio.Writer
	Mutex      *sync.Mutex
	Collocated bool
}

type GBeacon struct {
	Rid       int32
	Timestamp int64
}

type Replica struct {
	M  sync.Mutex
	N  int
	F  int
	Id int32

	PeerAddrList       []string
	Peers              []net.Conn
	PeerReaders        []*bufio.Reader
	PeerWriters        []*bufio.Writer
	ClientWriters      map[int32]*bufio.Writer
	ProxyAddrs         map[string]struct{}
	Alive              []bool
	PreferredPeerOrder []int32

	State       *state.State
	RPC         *fastrpc.Table
	StableStore *os.File
	Stats       *Stats
	Shutdown    bool
	Listener    net.Listener
	ProposeChan chan *GPropose
	BeaconChan  chan *GBeacon

	Thrifty bool
	Exec    bool
	LRead   bool
	Dreply  bool
	Beacon  bool
	Durable bool

	Ewma      []float64
	Latencies []int64
}

const (
	CHAN_BUFFER_SIZE = 2000000
	TRUE             = uint8(1)
	FALSE            = uint8(0)
)

var (
	Storage      = ""
	StoreFilname = "stable_store"
)

func NewReplica(id, f int, addrs []string, thrifty, exec, lread, drep bool, ps map[string]struct{}) *Replica {
	n := len(addrs)
	r := &Replica{
		N:  n,
		F:  f,
		Id: int32(id),

		PeerAddrList:       addrs,
		Peers:              make([]net.Conn, n),
		PeerReaders:        make([]*bufio.Reader, n),
		PeerWriters:        make([]*bufio.Writer, n),
		ClientWriters:      make(map[int32]*bufio.Writer),
		ProxyAddrs:         ps,
		Alive:              make([]bool, n),
		PreferredPeerOrder: make([]int32, n),

		State:       state.InitState(),
		RPC:         fastrpc.NewTableId(RPC_TABLE),
		StableStore: nil,
		Stats:       &Stats{make(map[string]int)},
		Shutdown:    false,
		Listener:    nil,
		ProposeChan: make(chan *GPropose, CHAN_BUFFER_SIZE),
		BeaconChan:  make(chan *GBeacon, CHAN_BUFFER_SIZE),

		Thrifty: thrifty,
		Exec:    exec,
		LRead:   lread,
		Dreply:  drep,
		Beacon:  false,
		Durable: false,

		Ewma:      make([]float64, n),
		Latencies: make([]int64, n),
	}

	var err error
	r.StableStore, err = os.Create(storeFullFileName(id))
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
		r.Latencies[i] = 0
	}

	return r
}

func (r *Replica) Ping(args *PingArgs, reply *PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *BeTheLeaderArgs, reply *BeTheLeaderReply) error {
	return nil
}

func (r *Replica) FastQuorumSize() int {
	return r.F + (r.F+1)/2
}

func (r *Replica) SlowQuorumSize() int {
	return (r.N + 1) / 2
}

func (r *Replica) WriteQuorumSize() int {
	return r.F + 1
}

func (r *Replica) ReadQuorumSize() int {
	return r.N - r.F
}

func (r *Replica) ConnectToPeers() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	for i := 0; i < int(r.Id); i++ {
		for {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				break
			}
			time.Sleep(1e9)
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			log.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
		log.Printf("OUT Connected to %d", i)
	}
	<-done
	log.Printf("Replica %d: done connecting to peers", r.Id)
	log.Printf("Node list %v", r.PeerAddrList)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) WaitForClientConnections() {
	log.Println("Waiting for client connections")

	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)
	}
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendClientMsg(id int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.ClientWriters[id]
	if w == nil {
		log.Printf("Connection to client %d lost!", id)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) ReplyProposeTS(reply *ProposeReplyTS, w *bufio.Writer, lock *sync.Mutex) {
	r.M.Lock()
	defer r.M.Unlock()

	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) SendBeacon(peerId int32) {
	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(GENERIC_SMR_BEACON)
	beacon := &Beacon{
		Timestamp: time.Now().UnixNano(),
	}
	beacon.Marshal(w)
	w.Flush()
	dlog.Println("send beacon", beacon.Timestamp, "to", peerId)
}

func (r *Replica) ReplyBeacon(beacon *GBeacon) {
	dlog.Println("replying beacon to", beacon.Rid)

	r.M.Lock()
	defer r.M.Unlock()

	w := r.PeerWriters[beacon.Rid]
	if w == nil {
		log.Printf("Connection to %d lost!", beacon.Rid)
		return
	}
	w.WriteByte(GENERIC_SMR_BEACON_REPLY)
	rb := &BeaconReply{
		Timestamp: beacon.Timestamp,
	}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.M.Lock()
	r.PreferredPeerOrder = aux
	r.M.Unlock()
}

func (r *Replica) ComputeClosestPeers() []float64 {
	npings := 20

	for j := 0; j < npings; j++ {
		for i := int32(0); i < int32(r.N); i++ {
			if i == r.Id {
				continue
			}
			r.M.Lock()
			if r.Alive[i] {
				r.M.Unlock()
				r.SendBeacon(i)
			} else {
				r.Latencies[i] = math.MaxInt64
				r.M.Unlock()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	quorum := make([]int32, r.N)

	r.M.Lock()
	for i := int32(0); i < int32(r.N); i++ {
		pos := 0
		for j := int32(0); j < int32(r.N); j++ {
			if (r.Latencies[j] < r.Latencies[i]) ||
				((r.Latencies[j] == r.Latencies[i]) && (j < i)) {
				pos++
			}
		}
		quorum[pos] = int32(i)
	}
	r.M.Unlock()

	r.UpdatePreferredPeerOrder(quorum)

	latencies := make([]float64, r.N-1)

	for i := 0; i < r.N-1; i++ {
		node := r.PreferredPeerOrder[i]
		lat := float64(r.Latencies[node]) / float64(npings*1000000)
		log.Println(node, "->", lat, "ms")
		latencies[i] = lat
	}

	return latencies
}

func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	port := strings.Split(r.PeerAddrList[r.Id], ":")[1]
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatal(r.PeerAddrList[r.Id], err)
	}
	r.Listener = l
	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			log.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
		log.Printf("IN Connected to %d", id)
	}

	done <- true
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var (
		msgType      uint8
		err          error = nil
		gbeacon      Beacon
		gbeaconReply BeaconReply
	)

	for err == nil && !r.Shutdown {
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			r.ReplyBeacon(&GBeacon{
				Rid:       int32(rid),
				Timestamp: gbeacon.Timestamp,
			})
			break

		case GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			dlog.Println("receive beacon", gbeaconReply.Timestamp, "reply from", rid)
			r.M.Lock()
			r.Latencies[rid] += time.Now().UnixNano() - gbeaconReply.Timestamp
			r.M.Unlock()
			now := time.Now().UnixNano()
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(now-gbeaconReply.Timestamp)
			break

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				go func() {
					p.Chan <- obj
				}()
			} else {
				log.Fatal("Error: received unknown message type ", msgType, " from ", rid)
			}
		}
	}

	r.M.Lock()
	r.Alive[rid] = false
	r.M.Unlock()
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var (
		msgType byte
		err     error
	)

	r.M.Lock()
	log.Println("Client up", conn.RemoteAddr(), "(", r.LRead, ")")
	r.M.Unlock()

	addr := strings.Split(conn.RemoteAddr().String(), ":")[0]
	_, isProxy := r.ProxyAddrs[addr]

	mutex := &sync.Mutex{}

	for !r.Shutdown && err == nil {
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {
		case PROPOSE:
			propose := &Propose{}
			if err = propose.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.ClientWriters[propose.ClientId] = writer
			r.M.Unlock()
			op := propose.Command.Op
			if r.LRead && (op == state.GET || op == state.SCAN) {
				r.ReplyProposeTS(&ProposeReplyTS{
					OK:        TRUE,
					CommandId: propose.CommandId,
					Value:     propose.Command.Execute(r.State),
					Timestamp: propose.Timestamp,
				}, writer, mutex)
			} else {
				go func(propose *GPropose) {
					r.ProposeChan <- propose
				}(&GPropose{
					Propose:    propose,
					Reply:      writer,
					Mutex:      mutex,
					Collocated: isProxy,
				})
			}
			break

		case READ:
			// TODO: do something with this
			read := &Read{}
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			break

		case PROPOSE_AND_READ:
			// TODO: do something with this
			pr := &ProposeAndRead{}
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			break

		case STATS:
			r.M.Lock()
			b, _ := json.Marshal(r.Stats)
			r.M.Unlock()
			writer.Write(b)
			writer.Flush()

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				go func() {
					p.Chan <- obj
				}()
			} else {
				log.Fatal("Error: received unknown client message ", msgType)
			}
		}
	}

	conn.Close()
	log.Println("Client down", conn.RemoteAddr())
}

func storeFullFileName(repId int) string {
	s := Storage
	if s == "" {
		s = "~"
	}
	return fmt.Sprintf("%v/%v-r%d", s, StoreFilname, repId)
}

func Leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func NextBallotOf(rid int32, oldBallot int32, repNum int) int32 {
	return (oldBallot/int32(repNum)+1)*int32(repNum) + rid
}
