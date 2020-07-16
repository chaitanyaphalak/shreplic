package paxoi

import (
	"log"
	"flag"
	"strings"

	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/server/smr"
)

type Client struct {
	*base.SimpleClient

	fastAndSlowAcks *smr.MsgSet

	N      int
	AQ     smr.Majority
	cs     CommunicationSupply
	ready  chan struct{}
	ballot int32
}

func NewClient(maddr, collocated string, mport, reqNum, writes, psize, conflict int,
	fast, lread, leaderless, verbose bool, logger *log.Logger, args string) *Client {

	// args must be of the form "-N <rep_num>"
	f := flag.NewFlagSet("custom Paxoi arguments", flag.ExitOnError)
	repNum := f.Int("N", 3, "Number of replicas")
	f.Parse(strings.Fields(args))

	c := &Client{
		SimpleClient: base.NewSimpleClient(maddr, collocated, mport, reqNum, writes,
			psize, conflict, fast, lread, leaderless, verbose, logger),

		N:      *repNum,
		AQ:     smr.NewMajorityOf(*repNum),
		ready:  make(chan struct{}, 1),
		ballot: -1,
	}

	c.ReadTable = true
	c.WaitResponse = func() error {
		<-c.ready
		c.reinitFastAndSlowAcks()
		return nil
	}

	initCs(&c.cs, c.RPC)

	go c.handleMsgs()

	return c
}

func (c *Client) reinitFastAndSlowAcks() {
	accept := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)
		return fastAck.Dep == nil ||
			(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
	}

	free := func(msg interface{}) {
		switch f := msg.(type) {
		case *MFastAck:
			releaseFastAck(f)
		}
	}

	c.fastAndSlowAcks.Free()
	c.fastAndSlowAcks = c.fastAndSlowAcks.ReinitMsgSet(c.AQ, accept, free,
		c.handleFastAndSlowAcks)
}

func (c *Client) handleMsgs() {
	for {
		select {
		case m := <-c.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			c.handleFastAck(fastAck)

		case m := <-c.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			c.handleSlowAck(slowAck)

		case m := <-c.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			c.handleLightSlowAck(lightSlowAck)

		case m := <-c.cs.acksChan:
			acks := m.(*MAcks)
			for _, f := range acks.FastAcks {
				c.handleFastAck(copyFastAck(&f))
			}
			for _, s := range acks.LightSlowAcks {
				ls := s
				c.handleLightSlowAck(&ls)
			}

		case m := <-c.cs.optAcksChan:
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
				c.handleFastAck(fastAck)
			}
		}
	}
}

func (c *Client) handleFastAck(f *MFastAck) {
	if c.ballot == -1 {
		c.ballot = f.Ballot
	} else if c.ballot < f.Ballot {
		c.ballot = f.Ballot
		c.reinitFastAndSlowAcks()
	} else if c.ballot > f.Ballot {
		return
	}

	c.fastAndSlowAcks.Add(f.Replica, false, f)
}

func (c *Client) handleSlowAck(s *MSlowAck) {
	c.handleFastAck((*MFastAck)(s))
}

func (c *Client) handleLightSlowAck(ls *MLightSlowAck) {
	f := newFastAck()
	f.Replica = ls.Replica
	f.Ballot = ls.Ballot
	f.CmdId = ls.CmdId
	f.Dep = nil
	c.handleFastAck(f)
}

func (c *Client) handleFastAndSlowAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}
}
