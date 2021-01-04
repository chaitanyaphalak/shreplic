package paxoi

import (
	"log"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

func (r *Replica) handleNewLeader(msg *MNewLeader) {
	if r.ballot >= msg.Ballot {
		return
	}
	log.Println("Recovering...")
	r.recNum++

	r.status = RECOVERING
	r.ballot = msg.Ballot
	r.recStart = time.Now()

	r.stopDescs()
	r.historyStart = r.gc.Stop()
	if !r.AQ.Contains(r.Id) {
		r.historyStart = r.historySize
	}

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

	if maxCbal == r.cballot && mAQ.Contains(r.Id) {
		r.handleShareState(shareState)
		return
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

	for newLeaderAck := range U {
		if newLeaderAck.Replica != r.Id {
			r.sender.SendTo(newLeaderAck.Replica, shareState, r.cs.shareStateRPC)
		} else {
			r.handleShareState(shareState)
		}
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

	/*for slot, sDesc := range r.history {
		if slot >= r.historySize {
			break
		}
		if sDesc.defered != nil {
			sDesc.defered()
		}
	}*/
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		v.(*commandDesc).defered()
	})

	for slot := r.historyStart; slot < r.historySize; slot++ {
		//_, exists := r.gc.pending[slot]
		//if exists {
		//	continue
		//}
		sDesc := r.history[slot]
		phases[sDesc.cmdId] = sDesc.phase
		cmds[sDesc.cmdId] = sDesc.cmd
		deps[sDesc.cmdId] = sDesc.dep
	}
	/*for slot, sDesc := range r.history {
		if slot >= r.historySize {
			break
		}
		phases[sDesc.cmdId] = sDesc.phase
		cmds[sDesc.cmdId] = sDesc.cmd
		deps[sDesc.cmdId] = sDesc.dep
	}*/
	// TODO: add in an order consistent with dep
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

	log.Println("totalSendNum:", len(cmds), r.historyStart)

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

	if r.status == NORMAL {
		r.gc.Stop()
		r.recStart = time.Now()
	}

	r.status = NORMAL
	r.ballot = msg.Ballot
	r.cballot = msg.Ballot
	r.AQ = r.qs.AQ(r.ballot)
	r.gc = NewGc(r)
	lv := r.dl.lastValue
	r.dl = NewDelayLog(r)
	r.dl.lastValue = lv

	r.stopDescs()
	// clear cmdDescs:
	r.cmdDescs.IterCb(func(cmdIdStr string, v interface{}) {
		desc := v.(*commandDesc)
		desc.msgs = nil
		desc.stopChan = nil
		desc.fastAndSlowAcks.Free()
		r.freeDesc(desc)
	})
	r.cmdDescs = cmap.New()

	committed := make(map[CommandId]struct{})
	clientCmds := make(map[int32]CommandId)

	for cmdId, phase := range msg.Phases {
		desc := r.getCmdDesc(cmdId, nil, nil)
		if desc != nil {
			desc.phase = phase
			desc.cmd = msg.Cmds[cmdId]
			desc.dep = msg.Deps[cmdId]
			desc.proposeDep = msg.Deps[cmdId]

			if phase == COMMIT {
				committed[cmdId] = struct{}{}
			} else if phase != ACCEPT {
				desc.phase = ACCEPT
			}
		}

		if propose, exists := r.proposes[cmdId]; exists {
			if desc != nil {
				desc.propose = propose
			}
			oldCmdId, exists := clientCmds[cmdId.ClientId]
			if !exists || oldCmdId.SeqNum < cmdId.SeqNum {
				clientCmds[cmdId.ClientId] = cmdId
			}
		}
	}

	for _, cmdId := range clientCmds {
		if r.AQ.Contains(r.Id) {
			propose := r.proposes[cmdId]
			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   cmdId,
			}
			if !r.optExec || r.Id == r.leader() {
				r.batcher.SendLightSlowAck(lightSlowAck)
				reply := &MReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   cmdId,
					Dep:     msg.Deps[cmdId],
					Rep:     state.NIL(),
				}
				r.sender.SendToClient(propose.ClientId, reply, r.cs.replyRPC)
			} else {
				r.batcher.SendLightSlowAckClient(lightSlowAck, propose.ClientId)
			}
			desc := r.getCmdDesc(cmdId, nil, nil)
			if desc != nil {
				defer r.handleLightSlowAck(lightSlowAck, desc)
			}
		}
	}

	go func() {
		for committedCmdId := range committed {
			r.deliverChan <- committedCmdId
		}
	}()

	log.Println("Recovered!")
	log.Println("Ballot:", r.ballot)
	log.Println("AQ:", r.AQ)
	log.Println("recovered in", time.Now().Sub(r.recStart))
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
	free := func(_ interface{}) {}
	Q := smr.NewMajorityOf(r.N)
	r.newLeaderAcks = r.newLeaderAcks.ReinitMsgSet(Q, accept, free, r.handleNewLeaderAcks)
}
