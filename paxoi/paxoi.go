package paxoi

import (
	"fmt"
	"log"
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

	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	gc      *gc
	sender  smr.Sender
	batcher *Batcher
	repchan *replyChan

	keys         map[state.Key]keyInfo
	reads        map[CommandId]*readDesc
	history      []commandStaticDesc
	historySize  int
	historyStart int

	AQ smr.Quorum
	qs *smr.QuorumSystem
	cs CommunicationSupply

	optExec     bool
	fastRead    bool
	deliverChan chan CommandId

	descPool     sync.Pool
	poolLevel    int
	routineCount int

	dl            *DelayLog
	recNum        int
	recover       chan int32
	newLeaderAcks *smr.MsgSet

	// TODO: get rid of this
	proposes map[CommandId]*smr.GPropose
}

type commandDesc struct {
	phase      int
	cmd        state.Command
	dep        Dep
	propose    *smr.GPropose
	proposeDep Dep

	fastAndSlowAcks *smr.MsgSet
	afterPropagate  *tools.OptCondF

	msgs     chan interface{}
	active   bool
	slowPath bool
	seq      bool
	stopChan chan *sync.WaitGroup

	successors  []CommandId
	successorsL sync.Mutex

	// execute before sending a MSync message
	defered func()
}

type commandStaticDesc struct {
	cmdId    CommandId
	phase    int
	cmd      state.Command
	dep      Dep
	slowPath bool
	defered  func()
}

type readDesc struct {
	dep     Dep
	propose *smr.GPropose
}

func NewReplica(rid int, addrs []string, exec, fastRead, dr, optExec bool,
	pl, f int, qfile string, ps map[string]struct{}) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: smr.NewReplica(rid, f, addrs, false, exec, false, dr, ps),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		cmdDescs:  cmap.New(),
		delivered: cmap.New(),

		keys:         make(map[state.Key]keyInfo),
		reads:        make(map[CommandId]*readDesc),
		history:      make([]commandStaticDesc, HISTORY_SIZE),
		historySize:  0,
		historyStart: 0,

		optExec:     optExec,
		fastRead:    fastRead,
		deliverChan: make(chan CommandId, smr.CHAN_BUFFER_SIZE),

		poolLevel:    pl,
		routineCount: 0,

		recNum:  0,
		recover: make(chan int32, 8),

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},

		proposes: make(map[CommandId]*smr.GPropose),
	}

	useFastAckPool = pl > 1

	r.sender = smr.NewSender(r.Replica)
	r.batcher = NewBatcher(r, 16, releaseFastAck, func(_ *MLightSlowAck) {})
	r.repchan = NewReplyChan(r)

	qs, err := smr.NewQuorumSystem(r.N/2+1, r.Replica, qfile)
	if err != nil {
		log.Fatal(err)
	}
	r.qs = qs
	r.ballot = r.qs.BallotAt(0)
	if r.ballot == -1 {
		r.ballot = 0
	}
	r.cballot = r.ballot
	r.AQ = r.qs.AQ(r.ballot)
	r.gc = NewGc(r)
	r.dl = NewDelayLog(r)

	initCs(&r.cs, r.RPC)

	tools.HookUser1(func() {
		totalNum := 0
		slowPaths := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].dep == nil {
				continue
			}
			totalNum++
			if r.history[i].slowPath {
				slowPaths++
			}
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
		fmt.Printf("Number of slow paths: %d\n", slowPaths)
	})

	go r.run()

	return r
}

// TODO: do something more elegant
func (r *Replica) BeTheLeader(_ *smr.BeTheLeaderArgs, reply *smr.BeTheLeaderReply) error {
	if !r.delivered.IsEmpty() {
		r.recover <- r.qs.BallotAt(r.recNum + 1)
	} else {
		reply.Leader = r.leader()
	}
	reply.NextLeader = smr.Leader(r.qs.BallotAt(r.recNum+1), r.N)
	return nil
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
		case swap := <-r.dl.swap:
			if r.leader() != r.Id {
				continue
			}
			newAQ := smr.NewQuorum(r.AQ.Size())
			for rid := range r.AQ {
				if rid == swap.oldFast {
					newAQ[swap.newFast] = struct{}{}
				} else {
					newAQ[rid] = struct{}{}
				}
			}
			r.recover <- r.qs.BallotOf(r.Id, newAQ)
		case newBallot := <-r.recover:
			newLeader := &MNewLeader{
				Replica: r.Id,
				Ballot:  r.ballot,
			}
			if newBallot != -1 {
				if newBallot > newLeader.Ballot {
					newLeader.Ballot = newBallot
				} else {
					newLeader.Ballot = r.qs.SameHigher(newBallot, newLeader.Ballot)
				}
			} else {
				newLeader.Ballot = smr.NextBallotOf(r.Id, newLeader.Ballot, r.N)
			}
			for quorumIsAlive := false; !quorumIsAlive; {
				quorumIsAlive = true
				for rid := range r.qs.AQ(newLeader.Ballot) {
					if rid != r.Id && !r.Alive[rid] {
						newLeader.Ballot = smr.NextBallotOf(r.Id, newLeader.Ballot, r.N)
						quorumIsAlive = false
						break
					}
				}
			}
			r.sender.SendToAll(newLeader, r.cs.newLeaderRPC)
			r.reinitNewLeaderAcks()
			r.handleNewLeader(newLeader)

		case cmdId := <-r.deliverChan:
			if rDesc, exists := r.reads[cmdId]; exists {
				r.deliverReadDesc(rDesc, cmdId)
			} else {
				r.getCmdDesc(cmdId, "deliver", nil)
			}

		case propose := <-r.ProposeChan:
			cmdId.ClientId = propose.ClientId
			cmdId.SeqNum = propose.CommandId
			r.proposes[cmdId] = propose
			if r.fastRead && propose.Command.Op == state.GET {
				r.handleRead(cmdId, propose)
			} else {
				dep := func() Dep {
					if !r.AQ.Contains(r.Id) {
						return nil
					}
					return r.getDepAndUpdateInfo(propose.Command, cmdId)
				}()
				desc := r.getCmdDesc(cmdId, propose, dep)
				if desc == nil {
					log.Fatal("Got propose for the delivered command ", cmdId)
				}
			}

		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			r.dl.BTick(fastAck.Ballot, fastAck.Replica, true)
			r.getCmdDesc(fastAck.CmdId, fastAck, nil)

		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			r.dl.BTick(slowAck.Ballot, slowAck.Replica, true)
			r.getCmdDesc(slowAck.CmdId, slowAck, nil)

		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			r.dl.BTick(lightSlowAck.Ballot, lightSlowAck.Replica, true)
			r.getCmdDesc(lightSlowAck.CmdId, lightSlowAck, nil)

		case m := <-r.cs.acksChan:
			acks := m.(*MAcks)
			for _, f := range acks.FastAcks {
				r.dl.BTick(f.Ballot, f.Replica, true)
				r.getCmdDesc(f.CmdId, copyFastAck(&f), nil)
			}
			for _, s := range acks.LightSlowAcks {
				ls := s
				r.dl.BTick(ls.Ballot, ls.Replica, true)
				r.getCmdDesc(s.CmdId, &ls, nil)
			}

		case m := <-r.cs.optAcksChan:
			optAcks := m.(*MOptAcks)
			for _, ack := range optAcks.Acks {
				fastAck := newFastAck()
				fastAck.Replica = optAcks.Replica
				fastAck.Ballot = optAcks.Ballot
				fastAck.CmdId = ack.CmdId
				if !IsNilDepOfCmdId(ack.CmdId, ack.Dep) {
					fastAck.Dep = ack.Dep
				} else {
					fastAck.Dep = nil
				}
				r.dl.BTick(optAcks.Ballot, optAcks.Replica, true)
				r.getCmdDesc(fastAck.CmdId, fastAck, nil)
			}

		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			r.handleNewLeader(newLeader)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			r.handleSync(sync)

		case m := <-r.cs.flushChan:
			flush := m.(*MFlush)
			r.handleFlush(flush)

		case m := <-r.cs.collectChan:
			collect := m.(*MCollect)
			r.dl.BTick(collect.Ballot, collect.Replica, false)
			go r.handleCollect(collect)
		}
	}
}

func (r *Replica) handlePropose(msg *smr.GPropose, desc *commandDesc, cmdId CommandId) {
	if r.status != NORMAL || desc.phase != START || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !r.AQ.Contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		desc.afterPropagate.Recall()
		return
	}

	desc.dep = desc.proposeDep
	desc.phase = PRE_ACCEPT
	if desc.afterPropagate.Recall() && desc.slowPath {
		// in this case a process already sent a MSlowAck
		// message, hence, no need to send MFastAck
		return
	}

	fastAck := newFastAck()
	fastAck.Replica = r.Id
	fastAck.Ballot = r.ballot
	fastAck.CmdId = cmdId
	fastAck.Dep = desc.dep

	fastAckSend := copyFastAck(fastAck)
	if !r.optExec {
		r.batcher.SendFastAck(fastAckSend)
	} else {
		if r.Id == r.leader() {
			r.batcher.SendFastAck(fastAckSend)
			// TODO: save old state
			r.deliver(desc, cmdId)
		} else {
			r.batcher.SendFastAckClient(fastAckSend, msg.ClientId)
		}
	}
	r.handleFastAck(fastAck, desc)
}

func (r *Replica) handleRead(cmdId CommandId, msg *smr.GPropose) {
	if !r.AQ.Contains(r.Id) {
		return
	}

	if !msg.Collocated {
		slowAck := &MLightSlowAck{
			Replica: r.Id,
			Ballot:  r.ballot,
			CmdId:   cmdId,
		}
		r.sender.SendToClient(msg.ClientId, slowAck, r.cs.lightSlowAckRPC)
		return
	}

	rDesc := &readDesc{
		dep:     r.getDep(msg.Command),
		propose: msg,
	}
	r.reads[cmdId] = rDesc

	for _, depCmdId := range rDesc.dep {
		depDesc := r.getCmdDesc(depCmdId, nil, nil)
		if depDesc == nil {
			continue
		}
		depDesc.successorsL.Lock()
		depDesc.successors = append(depDesc.successors, cmdId)
		depDesc.successorsL.Unlock()
	}
	r.deliverReadDesc(rDesc, cmdId)
}

func (r *Replica) handleFastAck(msg *MFastAck, desc *commandDesc) {
	if msg.Replica == r.leader() {
		r.fastAckFromLeader(msg, desc)
	} else {
		r.commonCaseFastAck(msg, desc)
	}
}

func (r *Replica) fastAckFromLeader(msg *MFastAck, desc *commandDesc) {
	if !r.AQ.Contains(r.Id) {
		desc.afterPropagate.Call(func() {
			if r.status == NORMAL && r.ballot == msg.Ballot {
				desc.dep = msg.Dep
			}
			desc.fastAndSlowAcks.Add(msg.Replica, true, msg)
		})
		return
	}

	desc.afterPropagate.Call(func() {
		if r.status != NORMAL || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}
		//
		// seems to be satisfied already

		desc.phase = ACCEPT
		msgCmdId := msg.CmdId
		dep := Dep(msg.Dep)
		desc.fastAndSlowAcks.Add(msg.Replica, true, msg)
		if r.delivered.Has(msgCmdId.String()) {
			// since at this point msg can be already deallocated,
			// it is important to check the saved value,
			// all this can happen if desc.seq == true
			return
		}
		equals, diffs := desc.dep.EqualsAndDiff(dep)

		if !equals {
			oldDefered := desc.defered
			desc.defered = func() {
				for cmdId := range diffs {
					if r.delivered.Has(cmdId.String()) {
						continue
					}
					descPrime := r.getCmdDesc(cmdId, nil, nil)
					if descPrime.phase == PRE_ACCEPT {
						descPrime.phase = PAYLOAD_ONLY
					}
				}
				oldDefered()
			}

			desc.dep = dep
			desc.slowPath = true

			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msgCmdId,
			}

			if !r.optExec {
				r.batcher.SendLightSlowAck(lightSlowAck)
			} else {
				r.batcher.SendLightSlowAckClient(lightSlowAck, desc.propose.ClientId)
			}
			r.handleLightSlowAck(lightSlowAck, desc)
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.fastAndSlowAcks.Add(msg.Replica, msg.Replica == r.leader(), msg)
}

func getFastAndSlowAcksHandler(r *Replica, desc *commandDesc) smr.MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {

		if leaderMsg == nil {
			return
		}

		leaderFastAck := leaderMsg.(*MFastAck)

		desc.phase = COMMIT

		for _, depCmdId := range desc.dep {
			depDesc := r.getCmdDesc(depCmdId, nil, nil)
			if depDesc == nil {
				continue
			}
			depDesc.successorsL.Lock()
			depDesc.successors = append(depDesc.successors, leaderFastAck.CmdId)
			depDesc.successorsL.Unlock()
		}

		r.deliver(desc, leaderFastAck.CmdId)
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck, desc *commandDesc) {
	r.commonCaseFastAck((*MFastAck)(msg), desc)
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	fastAck := newFastAck()
	fastAck.Replica = msg.Replica
	fastAck.Ballot = msg.Ballot
	fastAck.CmdId = msg.CmdId
	fastAck.Dep = nil
	r.commonCaseFastAck(fastAck, desc)
}

func (r *Replica) handleFlush(msg *MFlush) {

}

func (r *Replica) handleCollect(msg *MCollect) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	r.gc.CollectAll(msg.Ids)
}

func (r *Replica) deliver(desc *commandDesc, cmdId CommandId) {
	// TODO: what if desc.propose is nil ?
	//       is that possible ?
	//
	//       Don't think so
	//       Now I do
	// TODO: How is that possible ?

	if desc.propose == nil || r.delivered.Has(cmdId.String()) || !r.Exec {
		return
	}

	if desc.phase != COMMIT && (!r.optExec || r.Id != r.leader()) {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	r.delivered.Set(cmdId.String(), struct{}{})

	if isNoop(desc.cmd) {
		return
	}

	dlog.Printf("Executing " + desc.cmd.String())
	v := desc.cmd.Execute(r.State)

	desc.successorsL.Lock()
	if desc.successors != nil {
		for _, sucCmdId := range desc.successors {
			go func(sucCmdId CommandId) {
				r.deliverChan <- sucCmdId
			}(sucCmdId)
		}
	}
	desc.successorsL.Unlock()

	if !r.Dreply {
		return
	}

	r.repchan.reply(desc, cmdId, v)
	if desc.seq {
		// wait for the slot number
		// and ignore any other message
		for {
			switch slot := (<-desc.msgs).(type) {
			case int:
				r.handleMsg(slot, desc, cmdId)
				return
			}
		}
	}
}

func (r *Replica) deliverReadDesc(rDesc *readDesc, cmdId CommandId) {
	if r.delivered.Has(cmdId.String()) || !r.Exec {
		return
	}

	for _, cmdIdPrime := range rDesc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	r.delivered.Set(cmdId.String(), struct{}{})
	dlog.Printf("Executing " + rDesc.propose.Command.String())
	v := rDesc.propose.Command.Execute(r.State)

	if !r.Dreply {
		return
	}

	r.repchan.readReply(rDesc.propose, cmdId, v)
}

func (r *Replica) getCmdDesc(cmdId CommandId, msg interface{}, dep Dep) *commandDesc {
	key := cmdId.String()
	if r.delivered.Has(key) {
		return nil
	}

	var desc *commandDesc

	r.cmdDescs.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			defer func() {
				if dep != nil && desc.proposeDep == nil {
					desc.proposeDep = dep
				}
			}()

			if exists {
				desc = mapV.(*commandDesc)
				return desc
			}

			desc = r.newDesc()
			if !desc.seq {
				go r.handleDesc(desc, cmdId)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, cmdId)
		} else {
			desc.msgs <- msg
		}
	}

	return desc
}

func (r *Replica) newDesc() *commandDesc {
	desc := r.allocDesc()
	desc.dep = nil
	if desc.msgs == nil {
		desc.msgs = make(chan interface{}, 8)
	}
	desc.active = true
	desc.phase = START
	desc.successors = nil
	desc.slowPath = false
	desc.seq = (r.routineCount >= MaxDescRoutines)
	desc.defered = func() {}
	desc.propose = nil
	desc.proposeDep = nil
	if desc.stopChan == nil {
		desc.stopChan = make(chan *sync.WaitGroup, 8)
	}

	desc.afterPropagate = desc.afterPropagate.ReinitCondF(func() bool {
		return desc.propose != nil
	})

	acceptFastAndSlowAck := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)
		return fastAck.Dep == nil ||
			(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
	}

	freeFastAck := func(msg interface{}) {
		switch f := msg.(type) {
		case *MFastAck:
			releaseFastAck(f)
		}
	}

	desc.fastAndSlowAcks = desc.fastAndSlowAcks.ReinitMsgSet(r.AQ,
		acceptFastAndSlowAck, freeFastAck,
		getFastAndSlowAcksHandler(r, desc))

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

func (r *Replica) handleDesc(desc *commandDesc, cmdId CommandId) {
	defer func() {
		for len(desc.stopChan) != 0 {
			(<-desc.stopChan).Done()
		}
	}()

	for desc.active {
		select {
		case wg := <-desc.stopChan:
			desc.active = false
			wg.Done()
			return
		case msg := <-desc.msgs:
			if r.handleMsg(msg, desc, cmdId) {
				r.routineCount--
				return
			}
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, cmdId CommandId) bool {
	switch msg := m.(type) {

	case *smr.GPropose:
		r.handlePropose(msg, desc, cmdId)

	case *MFastAck:
		if msg.CmdId == cmdId {
			r.handleFastAck(msg, desc)
		}

	case *MSlowAck:
		if msg.CmdId == cmdId {
			r.handleSlowAck(msg, desc)
		}

	case *MLightSlowAck:
		if msg.CmdId == cmdId {
			r.handleLightSlowAck(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, cmdId)
		}

	case int:
		r.history[msg].cmdId = cmdId
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		r.history[msg].dep = desc.dep
		r.history[msg].slowPath = desc.slowPath
		r.history[msg].defered = desc.defered
		desc.active = false
		desc.fastAndSlowAcks.Free()
		r.cmdDescs.Remove(cmdId.String())
		r.freeDesc(desc)
		r.gc.Record(cmdId, msg)
		return true
	}

	return false
}

func (r *Replica) leader() int32 {
	return smr.Leader(r.ballot, r.N)
}

func (r *Replica) getDep(cmd state.Command) Dep {
	dep := []CommandId{}
	keysOfCmd := keysOf(cmd)

	for _, key := range keysOfCmd {
		info, exists := r.keys[key]

		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		}
	}

	return dep
}

func (r *Replica) getDepAndUpdateInfo(cmd state.Command, cmdId CommandId) Dep {
	dep := []CommandId{}
	keysOfCmd := keysOf(cmd)

	for _, key := range keysOfCmd {
		info, exists := r.keys[key]

		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		} else {
			info = newLightKeyInfo()
			r.keys[key] = info
		}

		info.add(cmd, cmdId)
	}

	return dep
}
