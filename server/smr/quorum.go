package smr

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

type QuorumI interface {
	Size() int
	Contains(int32) bool
}

type Majority int

func NewMajorityOf(N int) Majority {
	return Majority(N/2+1)
}

func (m Majority) Size() int {
	return int(m)
}

func (m Majority) Contains(int32) bool {
	return true
}

type Quorum map[int32]struct{}

type QuorumsOfLeader map[int32]Quorum

type QuorumSet map[int32]QuorumsOfLeader

var NO_QUORUM_FILE = errors.New("Quorum file is not provided")

func NewQuorum(size int) Quorum {
	return make(map[int32]struct{}, size)
}

func NewQuorumOfAll(size int) Quorum {
	q := NewQuorum(size)

	for i := int32(0); i < int32(size); i++ {
		q[i] = struct{}{}
	}

	return q
}

func NewQuorumFromFile(qfile string, r *Replica) (Quorum, int32, error) {
	if qfile == "" {
		return Quorum{}, 0, NO_QUORUM_FILE
	}

	f, err := os.Open(qfile)
	if err != nil {
		return Quorum{}, 0, err
	}
	defer f.Close()

	leader := int32(0)
	AQ := NewQuorum(r.N/2 + 1)
	s := bufio.NewScanner(f)
	for s.Scan() {
		id := r.Id
		isLeader := false
		addr := ""

		data := strings.Split(s.Text(), " ")
		if len(data) == 1 {
			addr = data[0]
		} else {
			isLeader = true
			addr = data[1]
		}

		for rid := int32(0); rid < int32(r.N); rid++ {
			paddr := strings.Split(r.PeerAddrList[rid], ":")[0]
			if addr == paddr {
				id = rid
				break
			}
		}

		AQ[id] = struct{}{}
		if isLeader {
			leader = id
		}
	}

	return AQ, leader, s.Err()
}

func (q Quorum) Size() int {
	return len(q)
}

func (q Quorum) Contains(repId int32) bool {
	_, exists := q[repId]
	return exists
}

func (q Quorum) copy() Quorum {
	nq := NewQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func NewQuorumsOfLeader() QuorumsOfLeader {
	return make(map[int32]Quorum)
}

func NewQuorumSet(quorumSize, repNum int) QuorumSet {
	ids := make([]int32, repNum)
	q := NewQuorum(quorumSize)
	qs := make(map[int32]QuorumsOfLeader, repNum)

	for id := range ids {
		ids[id] = int32(id)
		qs[int32(id)] = NewQuorumsOfLeader()
	}

	subsets(ids, repNum, quorumSize, 0, q, qs)

	return qs
}

func (qs QuorumSet) AQ(ballot int32) Quorum {
	l := Leader(ballot, len(qs))
	lqs := qs[l]
	qid := (ballot / int32(len(qs))) % int32(len(lqs))
	return lqs[qid]
}

func subsets(ids []int32, repNum, quorumSize, i int,
	q Quorum, qs QuorumSet) {

	if quorumSize == 0 {
		for repId := int32(0); repId < int32(repNum); repId++ {
			length := int32(len(qs[repId]))
			_, exists := q[repId]
			if exists {
				qs[repId][length] = q.copy()
			}
		}
	}

	for j := i; j < repNum; j++ {
		q[ids[j]] = struct{}{}
		subsets(ids, repNum, quorumSize-1, j+1, q, qs)
		delete(q, ids[j])
	}
}
