package tools

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Probe struct {
	name string
	n    int64

	totalDuration time.Duration
	minDuration   time.Duration
	maxDuration   time.Duration

	startTime time.Time
}

func NewProbe(name string) *Probe {
	p := &Probe{
		name:        name,
		minDuration: -1,
	}

	HookUser1(func() {
		fmt.Println(p)
	})

	return p
}

func (p *Probe) Start() {
	p.startTime = time.Now()
}

func (p *Probe) Stop() {
	endTime := time.Now()
	duration := endTime.Sub(p.startTime)

	if duration > p.maxDuration {
		p.maxDuration = duration
	}

	if duration < p.minDuration || p.minDuration == -1 {
		p.minDuration = duration
	}

	p.totalDuration = p.totalDuration + duration
	p.n++
}

func (p *Probe) String() string {
	average := time.Duration(0)
	if p.n != 0 {
		average = p.totalDuration / time.Duration(p.n)
	}

	return fmt.Sprintf("%s\n"+
		"total duration:   %v\n"+
		"average duration: %v\n"+
		"min duration:     %v\n"+
		"max duration:     %v\n"+
		"number of calls:  %v\n",
		p.name, p.totalDuration, average,
		p.minDuration, p.maxDuration, p.n)
}

func HookUser1(f func()) {
	user1 := make(chan os.Signal, 1)
	signal.Notify(user1, syscall.SIGUSR1)

	go func() {
		<-user1
		f()
	}()
}
