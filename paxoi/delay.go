package paxoi

import (
	"time"
)

const (
	BAD_CONT  = 4
	THRESHOLD = time.Duration(500 * time.Millisecond)
)

type DelayEntry struct {
	now      time.Time
	badCount int
}

type SwapValue struct {
	newFast int32
	oldFast int32
}

type DelayLog struct {
	log           []DelayEntry
	fastestSlowD  time.Duration
	fastestSlowId int32
}

func NewDelayLog(repNum int) *DelayLog {
	dl := &DelayLog{
		log:           make([]DelayEntry, repNum),
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

	if fast && dl.log[i].badCount > BAD_CONT &&
		dl.fastestSlowD < d && dl.fastestSlowId != -1 {
		return dl.fastestSlowId
	}
	if !fast && (d < dl.fastestSlowD || dl.fastestSlowId == -1) {
		dl.fastestSlowId = id
	}
	return id
}
