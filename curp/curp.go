package curp

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
	"github.com/vonaka/shreplic/tools"
	"github.com/vonaka/shreplic/tools/dlog"
)

type Replica struct {
	*smr.Replica

	ballot  int32
	cballot int32
	status  int

	Q smr.Majority

	isLeader    bool
	lastCmdSlot int

	slots     map[CommandId]int
	synced    cmap.ConcurrentMap
	values    cmap.ConcurrentMap
	proposes  cmap.ConcurrentMap
	cmdDescs  cmap.ConcurrentMap
	unsynced  cmap.ConcurrentMap
	executed  cmap.ConcurrentMap
	committed cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	sender  smr.Sender
	history []commandStaticDesc

	AQ smr.Quorum
	qs smr.QuorumSet
	cs CommunicationSupply

	deliverChan chan int

	descPool     sync.Pool
	poolLevel    int
	routineCount int
}

type commandDesc struct {
	cmdId CommandId

	cmd     state.Command
	sent    bool
	phase   int
	cmdSlot int
	propose *smr.GPropose
	val     []byte

	dep        int
	successor  int
	successorL sync.Mutex

	acks         *smr.MsgSet
	afterPayload *tools.OptCondF

	msgs   chan interface{}
	active bool
	seq    bool
}

type commandStaticDesc struct {
	cmdSlot int
	phase   int
	cmd     state.Command
}

func NewReplica(rid int, addrs []string, exec, dr bool,
	pl, f int, qfile string, ps map[string]struct{}) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: smr.NewReplica(rid, f, addrs, false, exec, false, dr, ps),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		isLeader:    false,
		lastCmdSlot: 0,

		slots:     make(map[CommandId]int),
		synced:    cmap.New(),
		values:    cmap.New(),
		proposes:  cmap.New(),
		cmdDescs:  cmap.New(),
		unsynced:  cmap.New(),
		executed:  cmap.New(),
		committed: cmap.New(),
		delivered: cmap.New(),
		history:   make([]commandStaticDesc, HISTORY_SIZE),

		deliverChan: make(chan int, smr.CHAN_BUFFER_SIZE),

		poolLevel:    pl,
		routineCount: 0,

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},
	}

	r.Q = smr.NewMajorityOf(r.N)
	r.sender = smr.NewSender(r.Replica)
	r.qs = smr.NewQuorumSet(r.N/2+1, r.N)

	_, leaderId, err := smr.NewQuorumFromFile(qfile, r.Replica)
	if err == nil {
		r.ballot = leaderId
		r.cballot = leaderId
		r.isLeader = (leaderId == r.Id)
	} else if err == smr.NO_QUORUM_FILE {
		r.isLeader = (r.ballot == r.Id)
	} else {
		log.Fatal(err)
	}

	initCs(&r.cs, r.RPC)

	tools.HookUser1(func() {
		totalNum := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].phase == 0 {
				continue
			}
			totalNum++
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
	})

	go r.run()

	return r
}

func (r *Replica) run() {
	r.ConnectToPeers()
	latencies := r.ComputeClosestPeers()
	for _, l := range latencies {
		d := time.Duration(l*1000*1000) * time.Nanosecond
		if d > r.cs.maxLatency {
			r.cs.maxLatency = d
		}
	}

	go r.WaitForClientConnections()

	var cmdId CommandId
	for !r.Shutdown {
		select {
		case int := <-r.deliverChan:
			r.getCmdDesc(int, "deliver", -1)

		case propose := <-r.ProposeChan:
			if r.isLeader {
				dep := r.leaderUnsync(propose.Command, r.lastCmdSlot)
				desc := r.getCmdDesc(r.lastCmdSlot, propose, dep)
				if desc == nil {
					log.Fatal("Got propose for the delivered command:",
						propose.ClientId, propose.CommandId)
				}
				r.lastCmdSlot++
			} else {
				cmdId.ClientId = propose.ClientId
				cmdId.SeqNum = propose.CommandId
				r.proposes.Set(cmdId.String(), propose)
				recAck := &MRecordAck{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   cmdId,
					Ok:      r.ok(propose.Command),
				}
				r.sender.SendToClient(propose.ClientId, recAck, r.cs.recordAckRPC)
				r.unsync(propose.Command)
				slot, exists := r.slots[cmdId]
				if exists {
					r.getCmdDesc(slot, "deliver", -1)
				}
			}

		case m := <-r.cs.acceptChan:
			acc := m.(*MAccept)
			r.slots[acc.CmdId] = acc.CmdSlot
			r.getCmdDesc(acc.CmdSlot, acc, -1)

		case m := <-r.cs.acceptAckChan:
			ack := m.(*MAcceptAck)
			r.getCmdDesc(ack.CmdSlot, ack, -1)

		case m := <-r.cs.aacksChan:
			aacks := m.(*MAAcks)
			for _, a := range aacks.Accepts {
				ta := a
				r.getCmdDesc(a.CmdSlot, &ta, -1)
			}
			for _, b := range aacks.Acks {
				tb := b
				r.getCmdDesc(b.CmdSlot, &tb, -1)
			}

		case m := <-r.cs.commitChan:
			commit := m.(*MCommit)
			r.getCmdDesc(commit.CmdSlot, commit, -1)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			val, exists := r.values.Get(sync.CmdId.String())
			if exists {
				rep := &MSyncReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   sync.CmdId,
					Rep:     val.([]byte),
				}
				r.sender.SendToClient(sync.CmdId.ClientId, rep, r.cs.syncReplyRPC)
			}
		}
	}
}

func (r *Replica) handlePropose(msg *smr.GPropose, desc *commandDesc, slot int, dep int) {

	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command
	desc.cmdId = CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}
	desc.cmdSlot = slot
	desc.dep = dep
	if dep != -1 {
		depDesc := r.getCmdDesc(dep, nil, -1)
		if depDesc != nil {
			depDesc.successorL.Lock()
			depDesc.successor = slot
			depDesc.successorL.Unlock()
		}
	}

	acc := &MAccept{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cmd:     desc.cmd,
		CmdId:   desc.cmdId,
		CmdSlot: slot,
	}

	r.deliver(desc, slot)
	r.sender.SendToAll(acc, r.cs.acceptRPC)
	r.handleAccept(acc, desc)
}

func (r *Replica) handleAccept(msg *MAccept, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.cmd = msg.Cmd
	desc.cmdId = msg.CmdId
	desc.cmdSlot = msg.CmdSlot

	defer desc.afterPayload.Recall()

	ack := &MAcceptAck{
		Replica: r.Id,
		Ballot:  msg.Ballot,
		CmdSlot: msg.CmdSlot,
	}

	if r.isLeader {
		r.handleAcceptAck(ack, desc)
	} else {
		r.sender.SendTo(msg.Replica, ack, r.cs.acceptAckRPC)
	}
}

func (r *Replica) handleAcceptAck(msg *MAcceptAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.acks.Add(msg.Replica, false, msg)
}

func getAcksHandler(r *Replica, desc *commandDesc) smr.MsgSetHandler {
	return func(_ interface{}, _ []interface{}) {
		commit := &MCommit{
			Replica: r.Id,
			Ballot:  r.ballot,
			CmdSlot: desc.cmdSlot,
		}
		r.sender.SendToAll(commit, r.cs.commitRPC)
		r.handleCommit(commit, desc)
	}
}

func (r *Replica) handleCommit(msg *MCommit, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot || desc.phase == COMMIT {
		return
	}

	desc.phase = COMMIT
	if r.isLeader {
		r.committed.Set(strconv.Itoa(desc.cmdSlot), struct{}{})
	} else {
		desc.afterPayload.Call(func() {
			r.sync(desc.cmdId, desc.cmd)
		})
	}
	defer func() {
		desc.successorL.Lock()
		succ := desc.successor
		desc.successorL.Unlock()
		if succ != -1 {
			go func() {
				r.deliverChan <- succ
			}()
		}
	}()
	r.deliver(desc, desc.cmdSlot)
}

func (r *Replica) sync(cmdId CommandId, cmd state.Command) {
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				if r.synced.Has(cmdId.String()) {
					return mapV
				}
				r.synced.Set(cmdId.String(), struct{}{})
				v := mapV.(int) - 1
				if v < 0 {
					v = 0
				}
				return v
			}
			r.synced.Set(cmdId.String(), struct{}{})
			return 0
		})
}

func (r *Replica) unsync(cmd state.Command) {
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				return mapV.(int) + 1
			}
			return 1
		})
}

func (r *Replica) leaderUnsync(cmd state.Command, slot int) int {
	depSlot := -1
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				if mapV.(int) > slot {
					return mapV
				}
				depSlot = mapV.(int)
			}
			return slot
		})
	return depSlot
}

func (r *Replica) ok(cmd state.Command) uint8 {
	key := strconv.FormatInt(int64(cmd.K), 10)
	v, exists := r.unsynced.Get(key)
	if exists && v.(int) > 0 {
		return FALSE
	}
	return TRUE
}

func (r *Replica) deliver(desc *commandDesc, slot int) {
	desc.afterPayload.Call(func() {
		slotStr := strconv.Itoa(slot)
		if !r.isLeader {
			r.sync(desc.cmdId, desc.cmd)
		}

		if r.delivered.Has(slotStr) || !r.Exec {
			return
		}

		if desc.phase != COMMIT && !r.isLeader {
			return
		}

		if slot > 0 && !r.executed.Has(strconv.Itoa(slot-1)) {
			return
		}

		p, exists := r.proposes.Get(desc.cmdId.String())
		if exists {
			desc.propose = p.(*smr.GPropose)
		}
		if desc.propose == nil {
			return
		}

		if desc.val == nil {
			dlog.Printf("Executing " + desc.cmd.String())
			desc.val = desc.cmd.Execute(r.State)
			r.executed.Set(slotStr, struct{}{})
			go func(nextSlot int) {
				r.deliverChan <- nextSlot
			}(slot + 1)
		}

		if r.isLeader {
			if desc.dep != -1 && !r.committed.Has(strconv.Itoa(desc.dep)) {
				return
			}

			if desc.phase == COMMIT {
				rep := &MSyncReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   desc.cmdId,
					Rep:     desc.val,
				}
				r.sender.SendToClient(desc.propose.ClientId, rep, r.cs.syncReplyRPC)
			} else {
				rep := &MReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   desc.cmdId,
					Rep:     desc.val,
				}
				r.sender.SendToClient(desc.propose.ClientId, rep, r.cs.replyRPC)
			}

			desc.sent = true
		}

		if desc.phase == COMMIT {
			desc.msgs <- slot
			r.delivered.Set(strconv.Itoa(slot), struct{}{})
			if desc.seq {
				for {
					switch hSlot := (<-desc.msgs).(type) {
					case int:
						r.handleMsg(hSlot, desc, slot, desc.dep)
						return
					}
				}
			}
		}
	})
}

func (r *Replica) getCmdDesc(slot int, msg interface{}, dep int) *commandDesc {
	slotStr := strconv.Itoa(slot)
	if r.delivered.Has(slotStr) {
		return nil
	}

	var desc *commandDesc

	r.cmdDescs.Upsert(slotStr, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				desc = mapV.(*commandDesc)
				return desc
			}

			desc = r.newDesc()
			desc.cmdSlot = slot
			if !desc.seq {
				go r.handleDesc(desc, slot, dep)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, slot, dep)
		} else {
			desc.msgs <- msg
		}
	}

	return desc
}

func (r *Replica) newDesc() *commandDesc {
	desc := r.allocDesc()
	desc.cmdSlot = -1
	if desc.msgs == nil {
		desc.msgs = make(chan interface{}, 8)
	}
	desc.active = true
	desc.phase = START
	desc.seq = (r.routineCount >= MaxDescRoutines)
	desc.sent = false
	desc.propose = nil
	desc.val = nil
	desc.cmdId.SeqNum = -42
	desc.dep = -1
	desc.successor = -1
	desc.successorL = sync.Mutex{}

	desc.afterPayload = desc.afterPayload.ReinitCondF(func() bool {
		return desc.cmdId.SeqNum != -42
	})

	desc.acks = desc.acks.ReinitMsgSet(r.Q, func(_, _ interface{}) bool {
		return true
	}, func(interface{}) {}, getAcksHandler(r, desc))

	return desc
}

func (r *Replica) allocDesc() *commandDesc {
	if r.poolLevel > 0 {
		return r.descPool.Get().(*commandDesc)
	}
	return &commandDesc{}
}

func (r *Replica) freeDesc(desc *commandDesc) {
	if r.poolLevel > 0 {
		r.descPool.Put(desc)
	}
}

func (r *Replica) handleDesc(desc *commandDesc, slot int, dep int) {
	for desc.active {
		if r.handleMsg(<-desc.msgs, desc, slot, dep) {
			r.routineCount--
			return
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, slot int, dep int) bool {
	switch msg := m.(type) {

	case *smr.GPropose:
		r.handlePropose(msg, desc, slot, dep)

	case *MAccept:
		if msg.CmdSlot == slot {
			r.handleAccept(msg, desc)
		}

	case *MAcceptAck:
		if msg.CmdSlot == slot {
			r.handleAcceptAck(msg, desc)
		}

	case *MCommit:
		if msg.CmdSlot == slot {
			r.handleCommit(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, slot)
		}

	case int:
		r.history[msg].cmdSlot = slot
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		desc.active = false
		slotStr := strconv.Itoa(slot)
		r.values.Set(desc.cmdId.String(), desc.val)
		r.cmdDescs.Remove(slotStr)
		r.freeDesc(desc)
		return true
	}

	return false
}
