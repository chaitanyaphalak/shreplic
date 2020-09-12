package paxoi

import (
	"sync"

	"github.com/vonaka/shreplic/server/smr"
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
}

func (r *Replica) reinitNewLeaderAcks() {
	accept := func(_, _ interface{}) bool {
		return true
	}
	free := func(_ interface{}) { }
	Q := smr.NewMajorityOf(r.N)
	r.newLeaderAcks = r.newLeaderAcks.ReinitMsgSet(Q, accept, free, r.handleNewLeaderAcks)
}
