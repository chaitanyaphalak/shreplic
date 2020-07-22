package curp

import (
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type Client struct {
	*base.SimpleClient

	acks *smr.MsgSet

	N      int
	Q      smr.ThreeQuarters
	cs     CommunicationSupply
	val    state.Value
	ready  chan struct{}
	leader int32
	ballot int32

	lock      sync.Mutex
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

		N:      *repNum,
		Q:      smr.NewThreeQuartersOf(*repNum),
		val:    nil,
		ready:  make(chan struct{}, 1),
		leader: -1,
		ballot: -1,
	}

	c.ReadTable = true
	c.WaitResponse = func() error {
		after := time.Duration(c.MaxLatency*float64(c.Q.Size())) * time.Millisecond
		cmdId := CommandId{
			ClientId: c.LastPropose.ClientId,
			SeqNum:   c.LastPropose.CommandId,
		}
		c.lock.Lock()
		c.lastCmdId = cmdId
		c.lock.Unlock()
		for stop := false; !stop; {
			select {
			case <-c.ready:
				stop = true
			case <-time.After(after):
				if c.leader != -1 {
					sync := &MSync{
						CmdId: cmdId,
					}
					c.SendMsg(c.leader, c.cs.syncRPC, sync)
				}
			}
		}
		c.reinitAcks()
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
			c.handleReply(rep)

		case m := <-c.cs.recordAckChan:
			recAck := m.(*MRecordAck)
			c.handleRecordAck(recAck, false)

		case m := <-c.cs.syncReplyChan:
			rep := m.(*MSyncReply)
			c.handleSyncReply(rep)
		}
	}
}

func (c *Client) handleReply(r *MReply) {
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
	if c.ballot == -1 {
		c.ballot = rep.Ballot
	} else if c.ballot < rep.Ballot {
		c.ballot = rep.Ballot
		c.reinitAcks()
	} else if c.ballot > rep.Ballot {
		return
	}
	c.leader = rep.Replica

	c.lock.Lock()
	if c.lastCmdId == rep.CmdId {
		c.lock.Unlock()
		c.val = state.Value(rep.Rep)
		c.Println("Returning:", c.val.String())
		c.ResChan <- c.val
		c.ready <- struct{}{}
	} else {
		c.lock.Unlock()
	}
}

func (c *Client) handleAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	c.Println("Returning:", c.val.String())
	c.ResChan <- c.val
	c.ready <- struct{}{}
}
