package paxoi

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
	"github.com/vonaka/shreplic/tools/fastrpc"
)

// status
const (
	NORMAL = iota
	RECOVERING
)

// phase
const (
	START = iota
	PAYLOAD_ONLY
	PRE_ACCEPT
	ACCEPT
	COMMIT
)

const HISTORY_SIZE = 10010001

var MaxDescRoutines = 100

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

func (cmdId CommandId) String() string {
	return fmt.Sprintf("%v,%v", cmdId.ClientId, cmdId.SeqNum)
}

type Dep []CommandId

func (d Dep) Contains(cmdId CommandId) bool {
	for _, c := range d {
		if c == cmdId {
			return true
		}
	}
	return false
}

func NilDepOfCmdId(cmdId CommandId) Dep {
	return []CommandId{cmdId}
}

func IsNilDepOfCmdId(cmdId CommandId, dep Dep) bool {
	return len(dep) == 1 && dep[0] == cmdId
}

func (dep1 Dep) Equals(dep2 Dep) bool {
	if len(dep1) != len(dep2) {
		return false
	}

	seen1 := make(map[CommandId]struct{})
	seen2 := make(map[CommandId]struct{})
	for i := 0; i < len(dep1); i++ {
		if dep1[i] == dep2[i] {
			continue
		}

		_, exists := seen2[dep1[i]]
		if exists {
			delete(seen2, dep1[i])
		} else {
			seen1[dep1[i]] = struct{}{}
		}

		_, exists = seen1[dep2[i]]
		if exists {
			delete(seen1, dep2[i])
		} else {
			seen2[dep2[i]] = struct{}{}
		}
	}

	return len(seen1) == len(seen2) && len(seen1) == 0
}

func (dep1 Dep) EqualsAndDiff(dep2 Dep) (bool, map[CommandId]struct{}) {
	seen1 := make(map[CommandId]struct{})
	seen2 := make(map[CommandId]struct{})

	for i := 0; i < len(dep1) || i < len(dep2); i++ {
		if i < len(dep1) && i < len(dep2) && dep1[i] == dep2[i] {
			continue
		}

		if i < len(dep1) {
			_, exists := seen2[dep1[i]]
			if exists {
				delete(seen2, dep1[i])
			} else {
				seen1[dep1[i]] = struct{}{}
			}
		}

		if i < len(dep2) {
			_, exists := seen1[dep2[i]]
			if exists {
				delete(seen1, dep2[i])
			} else {
				seen2[dep2[i]] = struct{}{}
			}
		}
	}

	return len(dep1) == len(dep2) && len(seen1) == len(seen2) &&
		len(seen1) == 0, seen1
}

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}

func isNoop(c state.Command) bool {
	return c.Op == state.NONE
}

func keysOf(cmd state.Command) []state.Key {
	switch cmd.Op {
	case state.SCAN:
		count := binary.LittleEndian.Uint64(cmd.V)
		ks := make([]state.Key, count)
		for i := range ks {
			ks[i] = cmd.K + state.Key(i)
		}
		return ks
	default:
		return []state.Key{cmd.K}
	}
}

type CommunicationSupply struct {
	maxLatency time.Duration

	fastAckChan      chan fastrpc.Serializable
	slowAckChan      chan fastrpc.Serializable
	lightSlowAckChan chan fastrpc.Serializable
	acksChan         chan fastrpc.Serializable
	optAcksChan      chan fastrpc.Serializable
	replyChan        chan fastrpc.Serializable
	readReplyChan    chan fastrpc.Serializable
	newLeaderChan    chan fastrpc.Serializable
	newLeaderAckChan chan fastrpc.Serializable
	shareStateChan   chan fastrpc.Serializable
	syncChan         chan fastrpc.Serializable
	pingChan         chan fastrpc.Serializable
	pingRepChan      chan fastrpc.Serializable
	collectChan      chan fastrpc.Serializable

	fastAckRPC      uint8
	slowAckRPC      uint8
	lightSlowAckRPC uint8
	acksRPC         uint8
	optAcksRPC      uint8
	replyRPC        uint8
	readReplyRPC    uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	shareStateRPC   uint8
	syncRPC         uint8
	pingRPC         uint8
	pingRepRPC      uint8
	collectRPC      uint8
}

func initCs(cs *CommunicationSupply, t *fastrpc.Table) {
	cs.maxLatency = 0

	cs.fastAckChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.slowAckChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.lightSlowAckChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.acksChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.optAcksChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.replyChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.readReplyChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.newLeaderChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.newLeaderAckChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.shareStateChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.syncChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.pingChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.pingRepChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)
	cs.collectChan = make(chan fastrpc.Serializable, smr.CHAN_BUFFER_SIZE)

	cs.fastAckRPC = t.Register(new(MFastAck), cs.fastAckChan)
	cs.slowAckRPC = t.Register(new(MSlowAck), cs.slowAckChan)
	cs.lightSlowAckRPC = t.Register(new(MLightSlowAck), cs.lightSlowAckChan)
	cs.acksRPC = t.Register(new(MAcks), cs.acksChan)
	cs.optAcksRPC = t.Register(new(MOptAcks), cs.optAcksChan)
	cs.replyRPC = t.Register(new(MReply), cs.replyChan)
	cs.readReplyRPC = t.Register(new(MReadReply), cs.readReplyChan)
	cs.newLeaderRPC = t.Register(new(MNewLeader), cs.newLeaderChan)
	cs.newLeaderAckRPC = t.Register(new(MNewLeaderAck), cs.newLeaderAckChan)
	cs.shareStateRPC = t.Register(new(MShareState), cs.shareStateChan)
	cs.syncRPC = t.Register(new(MSync), cs.syncChan)
	cs.pingRPC = t.Register(new(MPing), cs.pingChan)
	cs.pingRepRPC = t.Register(new(MPingRep), cs.pingRepChan)
	cs.collectRPC = t.Register(new(MCollect), cs.collectChan)
}

type keyInfo interface {
	add(state.Command, CommandId)
	remove(state.Command, CommandId)
	getConflictCmds(cmd state.Command) []CommandId
}

type fullKeyInfo struct {
	clientLastWrite []CommandId
	clientLastCmd   []CommandId
	lastWriteIndex  map[int32]int
	lastCmdIndex    map[int32]int
}

func newFullKeyInfo() *fullKeyInfo {
	return &fullKeyInfo{
		clientLastWrite: []CommandId{},
		clientLastCmd:   []CommandId{},
		lastWriteIndex:  make(map[int32]int),
		lastCmdIndex:    make(map[int32]int),
	}
}

func (ki *fullKeyInfo) add(cmd state.Command, cmdId CommandId) {
	cmdIndex, exists := ki.lastCmdIndex[cmdId.ClientId]

	if exists {
		ki.clientLastCmd[cmdIndex] = cmdId
	} else {
		ki.lastCmdIndex[cmdId.ClientId] = len(ki.clientLastCmd)
		ki.clientLastCmd = append(ki.clientLastCmd, cmdId)
	}

	if cmd.Op == state.PUT {
		writeIndex, exists := ki.lastWriteIndex[cmdId.ClientId]

		if exists {
			ki.clientLastWrite[writeIndex] = cmdId
		} else {
			ki.lastWriteIndex[cmdId.ClientId] = len(ki.clientLastWrite)
			ki.clientLastWrite = append(ki.clientLastWrite, cmdId)
		}
	}
}

func (ki *fullKeyInfo) remove(cmd state.Command, cmdId CommandId) {
	cmdIndex, exists := ki.lastCmdIndex[cmdId.ClientId]

	if exists {
		lastCmdIndex := len(ki.clientLastCmd) - 1
		lastCmdId := ki.clientLastCmd[lastCmdIndex]
		ki.lastCmdIndex[lastCmdId.ClientId] = cmdIndex
		ki.clientLastCmd[cmdIndex] = lastCmdId
		ki.clientLastCmd = ki.clientLastCmd[0:lastCmdIndex]
		delete(ki.lastCmdIndex, cmdId.ClientId)
	}

	if cmd.Op == state.PUT {
		writeIndex, exists := ki.lastWriteIndex[cmdId.ClientId]

		if exists {
			lastWriteIndex := len(ki.clientLastWrite) - 1
			lastWriteId := ki.clientLastWrite[lastWriteIndex]
			ki.lastWriteIndex[lastWriteId.ClientId] = writeIndex
			ki.clientLastWrite[writeIndex] = lastWriteId
			ki.clientLastWrite = ki.clientLastWrite[0:lastWriteIndex]
			delete(ki.lastWriteIndex, cmdId.ClientId)
		}
	}
}

func (ki *fullKeyInfo) getConflictCmds(cmd state.Command) []CommandId {
	if cmd.Op == state.GET {
		return ki.clientLastWrite
	} else {
		return ki.clientLastCmd
	}
}

type lightKeyInfo struct {
	lastWrite []CommandId
	lastCmd   []CommandId
}

func newLightKeyInfo() *lightKeyInfo {
	return &lightKeyInfo{
		lastWrite: []CommandId{},
		lastCmd:   []CommandId{},
	}
}

func (ki *lightKeyInfo) add(cmd state.Command, cmdId CommandId) {
	ki.lastCmd = []CommandId{cmdId}

	if cmd.Op == state.PUT {
		ki.lastWrite = []CommandId{cmdId}
	}
}

func (ki *lightKeyInfo) remove(_ state.Command, cmdId CommandId) {
	if len(ki.lastCmd) > 0 && ki.lastCmd[0] == cmdId {
		ki.lastCmd = []CommandId{}
	}

	if len(ki.lastWrite) > 0 && ki.lastWrite[0] == cmdId {
		ki.lastWrite = []CommandId{}
	}
}

func (ki *lightKeyInfo) getConflictCmds(cmd state.Command) []CommandId {
	if cmd.Op == state.GET {
		return ki.lastWrite
	} else {
		return ki.lastCmd
	}
}
