package paxoi

import (
	"flag"
	"log"
	"strings"

	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type Client struct {
	*base.SimpleClient

	fastAndSlowAcks *smr.MsgSet

	N         int
	AQ        smr.Majority
	cs        CommunicationSupply
	val       state.Value
	ready     chan struct{}
	ballot    int32
	delivered map[CommandId]struct{}

	slowPaths int
	alreadySlow map[CommandId]struct{}
}

func NewClient(maddr, collocated string, mport, reqNum, writes, psize, conflict int,
	fast, lread, leaderless, verbose bool, logger *log.Logger, args string) *Client {

	// args must be of the form "-N <rep_num>"
	f := flag.NewFlagSet("custom Paxoi arguments", flag.ExitOnError)
	repNum := f.Int("N", -1, "Number of replicas")
	f.Int("pclients", 0, "Number of clients already running on other machines")
	f.Parse(strings.Fields(args))
	if *repNum == -1 {
		f.Usage()
		return nil
	}

	c := &Client{
		SimpleClient: base.NewSimpleClient(maddr, collocated, mport, reqNum, writes,
			psize, conflict, fast, lread, leaderless, verbose, logger),

		N:         *repNum,
		AQ:        smr.NewMajorityOf(*repNum),
		val:       nil,
		ready:     make(chan struct{}, 1),
		ballot:    -1,
		delivered: make(map[CommandId]struct{}),

		slowPaths: 0,
		alreadySlow: make(map[CommandId]struct{}),
	}

	c.ReadTable = true
	c.WaitResponse = func() error {
		<-c.ready
		return nil
	}

	initCs(&c.cs, c.RPC)
	c.reinitFastAndSlowAcks()

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
		return fastAck.Dep == nil || (Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
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
			c.handleFastAck(fastAck, false)

		case m := <-c.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			if _, exists := c.alreadySlow[slowAck.CmdId]; !c.Reading && !exists {
				c.slowPaths++
				c.alreadySlow[slowAck.CmdId] = struct{}{}
			}
			c.handleSlowAck(slowAck)

		case m := <-c.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			if _, exists := c.alreadySlow[lightSlowAck.CmdId]; !c.Reading && !exists {
				c.slowPaths++
				c.alreadySlow[lightSlowAck.CmdId] = struct{}{}
			}
			c.handleLightSlowAck(lightSlowAck)

		case m := <-c.cs.acksChan:
			acks := m.(*MAcks)
			for _, f := range acks.FastAcks {
				c.handleFastAck(copyFastAck(&f), false)
			}
			for _, s := range acks.LightSlowAcks {
				ls := s
				if _, exists := c.alreadySlow[ls.CmdId]; !c.Reading && !exists {
					c.slowPaths++
					c.alreadySlow[ls.CmdId] = struct{}{}
				}
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
					if _, exists := c.alreadySlow[fastAck.CmdId]; !c.Reading && !exists {
						c.slowPaths++
						c.alreadySlow[fastAck.CmdId] = struct{}{}
					}
					fastAck.Dep = nil
				}
				c.handleFastAck(fastAck, false)
			}

		case m := <-c.cs.replyChan:
			reply := m.(*MReply)
			c.handleReply(reply)

		case m := <-c.cs.readReplyChan:
			reply := m.(*MReadReply)
			c.handleReadReply(reply)
		}
	}
}

func (c *Client) handleFastAck(f *MFastAck, fromLeader bool) {
	if c.ballot == -1 {
		c.ballot = f.Ballot
	} else if c.ballot < f.Ballot {
		c.ballot = f.Ballot
		c.reinitFastAndSlowAcks()
	} else if c.ballot > f.Ballot {
		return
	}

	if _, exists := c.delivered[f.CmdId]; exists {
		return
	}

	c.fastAndSlowAcks.Add(f.Replica, fromLeader, f)
}

func (c *Client) handleSlowAck(s *MSlowAck) {
	c.handleFastAck((*MFastAck)(s), false)
}

func (c *Client) handleLightSlowAck(ls *MLightSlowAck) {
	if _, exists := c.delivered[ls.CmdId]; exists {
		return
	}
	f := newFastAck()
	f.Replica = ls.Replica
	f.Ballot = ls.Ballot
	f.CmdId = ls.CmdId
	f.Dep = nil
	c.handleFastAck(f, false)
}

func (c *Client) handleFastAndSlowAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	cmdId := leaderMsg.(*MFastAck).CmdId
	if _, exists := c.delivered[cmdId]; exists {
		return
	}
	c.delivered[cmdId] = struct{}{}

	c.Println("Slow Paths:", c.slowPaths)
	println("Slow Paths:", c.slowPaths)
	c.Println("Returning:", c.val.String())
	c.reinitFastAndSlowAcks()
	c.ResChan <- c.val
	c.ready <- struct{}{}
}

func (c *Client) handleReply(r *MReply) {
	if _, exists := c.delivered[r.CmdId]; exists {
		return
	}
	f := newFastAck()
	f.Replica = r.Replica
	f.Ballot = r.Ballot
	f.CmdId = r.CmdId
	f.Dep = r.Dep
	c.val = r.Rep
	c.handleFastAck(f, true)
}

func (c *Client) handleReadReply(r *MReadReply) {
	if _, exists := c.delivered[r.CmdId]; exists {
		return
	}
	f := newFastAck()
	f.Replica = r.Replica
	f.Ballot = r.Ballot
	f.CmdId = r.CmdId
	f.Dep = nil
	c.val = r.Rep
	c.handleFastAck(f, true) // <-- this `true` is not a bug
}
