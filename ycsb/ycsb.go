package ycsb

import (
	"github.com/vonaka/shreplic/client/base"
	"github.com/vonaka/shreplic/curp"
	"github.com/vonaka/shreplic/paxoi"
)

type ShreplicClient interface {
	Connect() error
	Disconnect()
	Read(int64) []byte
	Scan(int64, int64) []byte
	Write(int64, []byte)
}

func NewShreplicClient(protocol, maddr, collocated string, mport int,
	fast, lread, leaderless, verbose bool, args string) ShreplicClient {

	var c ShreplicClient

	switch protocol {
	case "base":
		c = base.NewSimpleClient(maddr, collocated, mport,
			0, 0, 0, 0, fast, lread, leaderless, verbose, nil)
	case "paxoi":
		c = paxoi.NewClient(maddr, collocated, mport,
			0, 0, 0, 0, fast, lread, leaderless, verbose, nil, args)
	case "curp":
		c = curp.NewClient(maddr, collocated, mport,
			0, 0, 0, 0, fast, lread, leaderless, verbose, nil, args)
	}

	return c
}
