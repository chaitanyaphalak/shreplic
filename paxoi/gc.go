package paxoi

import (
	"sync"

	"github.com/orcaman/concurrent-map"
	"github.com/vonaka/shreplic/server/smr"
)

const COLLECT_SIZE = 2000

type gc struct {
	trash  chan int
	slots  cmap.ConcurrentMap
	ackNum int
	active bool

	next     int
	mcollect MCollect

	wg           sync.WaitGroup
	pending      map[int]struct{}
	historyStart int
}

type slotInfo struct {
	ackNum       int
	historyIndex int
}

func NewGc(r *Replica) *gc {
	g := &gc{
		// smr.CHAN_BUFFER_SIZE ? Do we need something that big?
		trash:  make(chan int, smr.CHAN_BUFFER_SIZE),
		slots:  cmap.New(),
		ackNum: r.N - r.AQ.Size() + 1,

		pending:      make(map[int]struct{}),
		historyStart: 0,
	}

	g.active = r.AQ.Contains(r.Id)

	if g.active {
		g.wg.Add(1)
		go func() {
			for {
				slot := <-g.trash
				if slot == -42 {
					break
				} else if slot < 0 {
					continue
				}
				g.pending[slot] = struct{}{}
				if slot == g.historyStart {
					_, exists := g.pending[slot]
					for exists {
						slot++
						g.historyStart = slot
						_, exists = g.pending[slot]
					}
				}
			}
			g.wg.Done()
		}()
	} else {
		g.mcollect.Ballot = r.ballot
		g.mcollect.Replica = r.Id
		g.mcollect.Ids = make([]CommandId, COLLECT_SIZE)
	}

	return g
}

func (g *gc) Collect(cmdId CommandId) {
	g.Record(cmdId, -1)
}

func (g *gc) CollectAll(cmdIds []CommandId) {
	for _, cmdId := range cmdIds {
		g.Record(cmdId, -1)
	}
}

func (g *gc) Record(cmdId CommandId, slot int) {
	if !g.active {
		return
	}

	cmdSlot := slotInfo{
		ackNum:       1,
		historyIndex: slot,
	}

	g.slots.Upsert(cmdId.String(), nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if !exists {
				if cmdSlot.ackNum == g.ackNum {
					g.trash <- cmdSlot.historyIndex
				}
				return cmdSlot
			}
			cmdSlot = mapV.(slotInfo)
			cmdSlot.ackNum++
			if cmdSlot.historyIndex == -1 && slot != -1 {
				cmdSlot.historyIndex = slot
			}
			if cmdSlot.ackNum == g.ackNum {
				g.trash <- cmdSlot.historyIndex
			}
			return cmdSlot
		})
}

func (g *gc) Prepare(r *Replica, cmdId CommandId) {
	if !g.active {
		g.wg.Wait()
		g.mcollect.Ballot = r.ballot
		g.mcollect.Ids[g.next] = cmdId
		g.next++
		if g.next == len(g.mcollect.Ids) {
			g.next = 0
			g.wg.Add(1)
			r.sender.SendToQuorumAndFree(r.AQ, &g.mcollect, r.cs.collectRPC, func() {
				g.wg.Done()
			})
		}
	}
}

func (g *gc) Stop() int {
	if g.active {
		g.trash <- -42
	}
	g.wg.Wait()
	return g.historyStart
}
