package base

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type SimpleClient struct {
	*Client

	WaitResponse func() error

	reqNum   int
	writes   int
	psize    int
	conflict int
	writer   io.Writer
}

func NewSimpleClient(maddr, collocated string,
	mport, reqNum, writes, psize, conflict int,
	fast, lread, leaderless, verbose bool, logger *log.Logger) *SimpleClient {
	rand.Seed(time.Now().UnixNano())
	sc := &SimpleClient{
		Client: NewClientWithLog(maddr, mport, fast, lread, leaderless, verbose, logger),

		WaitResponse: nil,

		reqNum:   reqNum,
		writes:   writes,
		psize:    psize,
		conflict: conflict,
		writer:   logger.Writer(),
	}
	sc.Collocated(collocated)
	return sc
}

func (c *SimpleClient) Run() error {
	for try := 0; ; try++ {
		err := c.Connect()
		if err == nil {
			break
		}
		c.Disconnect()
		if try > 3 {
			return err
		}
	}
	c.Println("Client", c.ClientId, "is up")

	var (
		before      time.Time
		beforeTotal time.Time
	)
	clientKey := state.Key(uint64(uuid.New().Time()))
	for i := 0; i < c.reqNum+1; i++ {
		key := clientKey
		if randomTrue(c.conflict) {
			key = state.Key(42)
		}
		go func(i int) {
			if i == 1 {
				beforeTotal = time.Now()
			}
			before = time.Now()
			if randomTrue(c.writes) {
				value := make([]byte, c.psize)
				rand.Read(value)
				c.Write(key, state.Value(value))
			} else {
				c.Read(key)
			}
		}(i)
		<-c.Waiting
		if c.WaitResponse != nil {
			err := c.WaitResponse()
			if err != nil {
				return err
			}
		} else {
			var err error
			if c.Fast {
				err = c.waitReplies(c.ClosestId, c.Seqnum)
			} else {
				err = c.waitReplies(c.LastSubmitter, c.Seqnum)
			}
			if err != nil {
				c.Disconnect()
				return err
			}
		}
		after := time.Now()

		if i != 0 {
			duration := after.Sub(before)
			fmt.Fprintf(c.writer, "latency %v\n", to_ms(duration.Nanoseconds()))
			fmt.Fprintf(c.writer, "chain %d-1\n", int64(to_ms(after.UnixNano())))
		}
	}
	afterTotal := time.Now()
	fmt.Fprintf(c.writer, "Test took %v\n", afterTotal.Sub(beforeTotal))
	c.Disconnect()
	return nil
}

func (c *SimpleClient) waitReplies(rid int, cmdId int32) error {
	for {
		rep, err := c.ProposeReplyFrom(rid)
		if err != nil {
			return err
		}
		if rep.CommandId != cmdId {
			continue
		}
		if rep.OK == smr.TRUE {
			c.Println("Returning:", rep.Value.String())
			c.ResChan <- rep.Value
			break
		} else {
			return errors.New("Failed to receive a response.")
		}
	}
	return nil
}

func randomTrue(prob int) bool {
	if prob >= 100 {
		return true
	}
	if prob > 0 {
		return rand.Intn(100) <= prob
	}
	return false
}

func to_ms(nano int64) float64 {
	return float64(nano) / float64(time.Millisecond)
}
