package curp

import (
	"flag"
	"log"
	"strings"

	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/server/smr"
)

type Client struct {
	*base.SimpleClient

	acks *smr.MsgSet

	N      int
	Q      smr.ThreeQuarters
	cs     CommunicationSupply
	val    []byte
	ready  chan struct{}
	ballot int32
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
		ballot: -1,
	}

	c.ReadTable = true
	c.WaitResponse = func() error {
		<-c.ready
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
	c.val = r.Rep
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

	c.acks.Add(r.Replica, fromLeader, r)
}

func (c *Client) handleAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	c.ResChan <- c.val
	c.ready <- struct{}{}
}
