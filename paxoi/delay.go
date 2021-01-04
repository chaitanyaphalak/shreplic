package paxoi

import (
	"time"
)

const (
	BAD_CONT  = 3
	THRESHOLD = time.Duration(500 * time.Millisecond)
)

type DelayEntry struct {
	now      time.Time
	badCount int
}

type SwapValue struct {
	oldFast int32
	newFast int32
}

type DelayLog struct {
	id            int32
	log           []DelayEntry
	swap          chan SwapValue
	ballot        int32
	lastValue     SwapValue
	fastestSlowD  time.Duration
	fastestSlowId int32
}

func NewDelayLog(r *Replica) *DelayLog {
	dl := &DelayLog{
		id:            r.Id,
		log:           make([]DelayEntry, r.N),
		swap:          make(chan SwapValue, 4),
		ballot:        r.ballot,
		lastValue:     SwapValue{oldFast: -1, newFast: -1},
		fastestSlowId: -1,
	}
	for i := range dl.log {
		dl.log[i].badCount = -1
	}
	return dl
}

func (dl *DelayLog) Tick(id int32, fast bool) int32 {
	i := int32(id)

	if dl.log[i].badCount == -1 {
		dl.log[i].badCount = 0
		dl.log[i].now = time.Now()
		return id
	}

	now := time.Now()
	d := now.Sub(dl.log[i].now)
	dl.log[i].now = now
	if d > THRESHOLD {
		dl.log[i].badCount++
	} else if dl.log[i].badCount > 0 {
		dl.log[i].badCount--
	}

	if fast {
		if dl.log[i].badCount == BAD_CONT && dl.fastestSlowId != -1 {
			return dl.fastestSlowId
		}
		if dl.lastValue.newFast == id && dl.log[dl.lastValue.oldFast].badCount == 0 {
			return dl.lastValue.oldFast
		}
	}
	if !fast && (d < dl.fastestSlowD || dl.fastestSlowId == -1) {
		dl.fastestSlowId = id
	}
	return id
}

func (dl *DelayLog) BTick(ballot, id int32, fast bool) {
	if dl.id == id || dl.ballot != ballot {
		return
	}
	nid := dl.Tick(id, fast)
	if nid != id {
		dl.lastValue = SwapValue{
			oldFast: id,
			newFast: nid,
		}
		dl.swap <- dl.lastValue
	}
}
