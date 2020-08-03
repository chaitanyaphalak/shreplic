package curp

import (
	"flag"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type Client struct {
	*base.SimpleClient

	acks *smr.MsgSet

	N         int
	t         *Timer
	Q         smr.ThreeQuarters
	cs        CommunicationSupply
	val       state.Value
	ready     chan struct{}
	leader    int32
	ballot    int32
	waitTime  time.Duration
	delivered map[int32]struct{}

	lastCmdId CommandId
}

func NewClient(maddr, collocated string, mport, reqNum, writes, psize, conflict int,
	fast, lread, leaderless, verbose bool, logger *log.Logger, args string) *Client {

	// args must be of the form "-N <rep_num>"
	f := flag.NewFlagSet("custom Paxoi arguments", flag.ExitOnError)
	repNum := f.Int("N", -1, "Number of replicas")
	f.Parse(strings.Fields(args))
	if *repNum == -1 {
		f.Usage()
		return nil
	}

	c := &Client{
		SimpleClient: base.NewSimpleClient(maddr, collocated, mport, reqNum, writes,
			psize, conflict, fast, lread, leaderless, verbose, logger),

		N:         *repNum,
		t:         NewTimer(),
		Q:         smr.NewThreeQuartersOf(*repNum),
		val:       nil,
		ready:     make(chan struct{}, 1),
		leader:    -1,
		ballot:    -1,
		delivered: make(map[int32]struct{}),
	}

	c.lastCmdId = CommandId{
		ClientId: c.ClientId,
		SeqNum:   0,
	}

	c.ReadTable = true
	c.GetClientKey = func() state.Key {
		return state.Key(uint64(uuid.New().Time()))
	}

	first := true
	c.WaitResponse = func() error {
		if first {
			sort.Float64Slice(c.Ping).Sort()
			waitTime := time.Duration(c.Ping[c.Q.Size()-1]*2.05+25) * time.Millisecond
			if waitTime < 100*time.Millisecond {
				waitTime = 100 * time.Millisecond
			}
			c.waitTime = waitTime
			c.t.Start(waitTime)
			first = false
		}
		<-c.ready
		return nil
	}

	initCs(&c.cs, c.RPC)
	c.reinitAcks()

	go c.handleMsgs()

	return c
}

func (c *Client) reinitAcks() {
	accept := func(msg, _ interface{}) bool {
		return msg.(*MRecordAck).Ok == TRUE
	}

	c.acks.Free()
	c.acks = c.acks.ReinitMsgSet(c.Q, accept, func(interface{}) {}, c.handleAcks)
}

func (c *Client) handleMsgs() {
	for {
		select {
		case m := <-c.cs.replyChan:
			rep := m.(*MReply)
			if rep.CmdId == c.lastCmdId {
				c.handleReply(rep)
			}

		case m := <-c.cs.recordAckChan:
			recAck := m.(*MRecordAck)
			if recAck.CmdId == c.lastCmdId {
				c.handleRecordAck(recAck, false)
			}

		case m := <-c.cs.syncReplyChan:
			rep := m.(*MSyncReply)
			if rep.CmdId == c.lastCmdId {
				c.handleSyncReply(rep)
			}

		case needSync := <-c.t.c:
			if needSync && c.leader != -1 {
				if _, exists := c.delivered[c.lastCmdId.SeqNum]; exists {
					return
				}
				sync := &MSync{
					CmdId: c.lastCmdId,
				}
				c.SendMsg(c.leader, c.cs.syncRPC, sync)
			}
		}
	}
}

func (c *Client) handleReply(r *MReply) {
	if _, exists := c.delivered[r.CmdId.SeqNum]; exists {
		return
	}

	ack := &MRecordAck{
		Replica: r.Replica,
		Ballot:  r.Ballot,
		CmdId:   r.CmdId,
		Ok:      TRUE,
	}
	c.val = state.Value(r.Rep)
	c.handleRecordAck(ack, true)
}

func (c *Client) handleRecordAck(r *MRecordAck, fromLeader bool) {
	if c.ballot == -1 {
		c.ballot = r.Ballot
	} else if c.ballot < r.Ballot {
		c.ballot = r.Ballot
		c.reinitAcks()
	} else if c.ballot > r.Ballot {
		return
	}

	if fromLeader {
		c.leader = r.Replica
	}

	c.acks.Add(r.Replica, fromLeader, r)
}

func (c *Client) handleSyncReply(rep *MSyncReply) {
	if _, exists := c.delivered[rep.CmdId.SeqNum]; exists {
		return
	}

	if c.ballot == -1 {
		c.ballot = rep.Ballot
	} else if c.ballot < rep.Ballot {
		c.ballot = rep.Ballot
		c.reinitAcks()
	} else if c.ballot > rep.Ballot {
		return
	}
	c.leader = rep.Replica

	c.val = state.Value(rep.Rep)
	c.delivered[rep.CmdId.SeqNum] = struct{}{}
	c.lastCmdId.SeqNum++
	c.Println("Returning:", c.val.String())
	c.ResChan <- c.val
	c.ready <- struct{}{}
	c.reinitAcks()
	c.t.Reset(c.waitTime)
}

func (c *Client) handleAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	c.delivered[leaderMsg.(*MRecordAck).CmdId.SeqNum] = struct{}{}
	c.lastCmdId.SeqNum++
	c.Println("Returning:", c.val.String())
	c.ResChan <- c.val
	c.ready <- struct{}{}
	c.reinitAcks()
	c.t.Reset(c.waitTime)
}
