package curp

import (
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/tools/fastrpc"
)

type Batcher struct {
	accepts    chan BatcherOp
	acceptAcks chan BatcherOp
}

type BatcherOp struct {
	id  int32
	msg fastrpc.Serializable
	to  smr.SendType
}

func NewBatcher(r *Replica, size int) *Batcher {
	b := &Batcher{
		accepts:    make(chan BatcherOp, size),
		acceptAcks: make(chan BatcherOp, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case op := <-b.accepts:
				acc := op.msg.(*MAccept)

				aLen := len(b.accepts) + 1
				bLen := len(b.acceptAcks)
				aacks := &MAAcks{
					Accepts: make([]MAccept, aLen),
					Acks:    make([]MAcceptAck, bLen),
				}

				aacks.Accepts[0] = *acc
				for i := 1; i < aLen; i++ {
					opP := <-b.accepts
					acc := opP.msg.(*MAccept)
					aacks.Accepts[i] = *acc
				}
				for i := 0; i < bLen; i++ {
					opP := <-b.acceptAcks
					ack := opP.msg.(*MAcceptAck)
					aacks.Acks[i] = *ack
				}

				switch op.to {
				case smr.SEND_ALL:
					r.sender.SendToAll(aacks, r.cs.aacksRPC)
				case smr.SEND_CLIENT:
					r.sender.SendToClient(op.id, aacks, r.cs.aacksRPC)
				case smr.SEND_SINGLE:
					r.sender.SendTo(op.id, aacks, r.cs.aacksRPC)
				}

			case op := <-b.acceptAcks:
				ack := op.msg.(*MAcceptAck)

				aLen := len(b.accepts)
				bLen := len(b.acceptAcks) + 1
				aacks := &MAAcks{
					Accepts: make([]MAccept, aLen),
					Acks:    make([]MAcceptAck, bLen),
				}

				for i := 0; i < aLen; i++ {
					opP := <-b.accepts
					acc := opP.msg.(*MAccept)
					aacks.Accepts[i] = *acc
				}
				aacks.Acks[0] = *ack
				for i := 1; i < bLen; i++ {
					opP := <-b.acceptAcks
					ack := opP.msg.(*MAcceptAck)
					aacks.Acks[i] = *ack
				}

				switch op.to {
				case smr.SEND_ALL:
					r.sender.SendToAll(aacks, r.cs.aacksRPC)
				case smr.SEND_CLIENT:
					r.sender.SendToClient(op.id, aacks, r.cs.aacksRPC)
				case smr.SEND_SINGLE:
					r.sender.SendTo(op.id, aacks, r.cs.aacksRPC)
				}
			}
		}
	}()

	return b
}

func (b *Batcher) SendAccept(acc *MAccept, to smr.SendType, id int32) {
	b.accepts <- BatcherOp{
		id:  id,
		msg: acc,
		to:  to,
	}
}

func (b *Batcher) SendAcceptAck(ack *MAcceptAck, to smr.SendType, id int32) {
	b.acceptAcks <- BatcherOp{
		id:  id,
		msg: ack,
		to:  to,
	}
}
