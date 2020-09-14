package paxoi

import (
	"sync"

	"github.com/orcaman/concurrent-map"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

func (r *Replica) handleNewLeader(msg *MNewLeader) {
	if r.ballot >= msg.Ballot {
		return
	}

	r.status = RECOVERING
	r.ballot = msg.Ballot

	r.stopDescs()
	r.reinitNewLeaderAcks() //TODO: move this to ``recover()'' function

	newLeaderAck := &MNewLeaderAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cballot: r.cballot,
	}
	if msg.Replica != r.Id {
		r.sender.SendTo(msg.Replica, newLeaderAck, r.cs.newLeaderAckRPC)
	} else {
		r.handleNewLeaderAck(newLeaderAck)
	}

	// stop processing normal channels:
	for r.status == RECOVERING {
		select {
		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			r.handleNewLeader(newLeader)

		case m := <-r.cs.newLeaderAckChan:
			newLeaderAck := m.(*MNewLeaderAck)
			r.handleNewLeaderAck(newLeaderAck)

		case m := <-r.cs.shareStateChan:
			shareState := m.(*MShareState)
			r.handleShareState(shareState)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			r.handleSync(sync)
		}
	}
}

func (r *Replica) handleNewLeaderAck(msg *MNewLeaderAck) {
	if r.status != RECOVERING || r.ballot != msg.Ballot {
		return
	}

	r.newLeaderAcks.Add(msg.Replica, false, msg)
}

func (r *Replica) handleNewLeaderAcks(_ interface{}, msgs []interface{}) {
	maxCbal := int32(-1)
	var U map[*MNewLeaderAck]struct{}

	for _, msg := range msgs {
		newLeaderAck := msg.(*MNewLeaderAck)
		if maxCbal < newLeaderAck.Cballot {
			U = make(map[*MNewLeaderAck]struct{})
			maxCbal = newLeaderAck.Cballot
		}
		if maxCbal == newLeaderAck.Cballot {
			U[newLeaderAck] = struct{}{}
		}
	}

	mAQ := r.qs.AQ(maxCbal)
	shareState := &MShareState{
		Replica: r.Id,
		Ballot:  r.ballot,
	}

	for newLeaderAck := range U {
		if mAQ.Contains(newLeaderAck.Replica) {
			if newLeaderAck.Replica != r.Id {
				r.sender.SendTo(newLeaderAck.Replica, shareState, r.cs.shareStateRPC)
			} else {
				r.handleShareState(shareState)
			}
			return
		}
	}

	if maxCbal == r.cballot {
		r.handleShareState(shareState)
		return
	}

	for newLeaderAck := range U {
		r.sender.SendTo(newLeaderAck.Replica, shareState, r.cs.shareStateRPC)
	}
}

func (r *Replica) handleShareState(msg *MShareState) {
	if r.status != RECOVERING || r.ballot != msg.Ballot {
		return
	}

	// TODO: optimize

	phases := make(map[CommandId]int)
	cmds := make(map[CommandId]state.Command)
	deps := make(map[CommandId]Dep)

	for _, sDesc := range r.history {
		sDesc.defered()
	}
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		v.(*commandDesc).defered()
	})

	for _, sDesc := range r.history {
		phases[sDesc.cmdId] = sDesc.phase
		cmds[sDesc.cmdId] = sDesc.cmd
		deps[sDesc.cmdId] = sDesc.dep
	}
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.propose != nil {
			cmdId := CommandId{
				ClientId: desc.propose.ClientId,
				SeqNum:   desc.propose.CommandId,
			}
			phases[cmdId] = desc.phase
			cmds[cmdId] = desc.cmd
			deps[cmdId] = desc.dep
		}
	})

	sync := &MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  phases,
		Cmds:    cmds,
		Deps:    deps,
	}
	r.sender.SendToAll(sync, r.cs.syncRPC)
	r.handleSync(sync)
}

func (r *Replica) handleSync(msg *MSync) {
	if r.ballot > msg.Ballot || (r.ballot == msg.Ballot && r.status == NORMAL) {
		return
	}

	r.status = NORMAL
	r.ballot = msg.Ballot
	r.cballot = msg.Ballot
	r.AQ = r.qs.AQ(r.ballot)

	proposes := make(map[CommandId]*smr.GPropose)

	r.stopDescs()
	// clear cmdDescs:
	r.cmdDescs.IterCb(func(cmdIdStr string, v interface{}) {
		desc := v.(*commandDesc)
		go func(desc *commandDesc) {
			if desc.propose != nil {
				cmdId := CommandId{
					ClientId: desc.propose.ClientId,
					SeqNum:   desc.propose.CommandId,
				}
				proposes[cmdId] = desc.propose
			}
			desc.msgs = nil
			desc.stopChan = nil
			desc.fastAndSlowAcks.Free()
			r.freeDesc(desc)
		}(desc)
	})
	r.cmdDescs = cmap.New()

	committed := make(map[CommandId]struct{})

	for cmdId, phase := range msg.Phases {
		propose, exists := proposes[cmdId]
		if !exists {
			continue
		}

		desc := r.getCmdDesc(cmdId, nil, nil)
		if desc == nil {
			continue
		}
		desc.cmd = msg.Cmds[cmdId]
		desc.dep = msg.Deps[cmdId]
		desc.phase = phase
		desc.propose = propose
		desc.proposeDep = msg.Deps[cmdId]

		if phase == COMMIT {
			committed[cmdId] = struct{}{}
		} else if phase != ACCEPT {
			desc.phase = ACCEPT
		}
	}

	go func() {
		for committedCmdId := range committed {
			r.deliverChan <- committedCmdId
		}
	}()
}

func (r *Replica) stopDescs() {
	var wg sync.WaitGroup
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.active && !desc.seq {
			wg.Add(1)
			desc.stopChan <- &wg
		}
	})
	wg.Wait()

	// TODO: maybe add to history even if stopped this way ?
}

func (r *Replica) reinitNewLeaderAcks() {
	accept := func(_, _ interface{}) bool {
		return true
	}
	free := func(_ interface{}) { }
	Q := smr.NewMajorityOf(r.N)
	r.newLeaderAcks = r.newLeaderAcks.ReinitMsgSet(Q, accept, free, r.handleNewLeaderAcks)
}
