package paxoi

import "github.com/vonaka/shreplic/tools/fastrpc"

type Batcher struct {
	fastAcks      chan BatcherOp
	lightSlowAcks chan BatcherOp
}

type BatcherOp struct {
	cid          int32
	msg          fastrpc.Serializable
	sendToClient bool
}

func NewBatcher(r *Replica, size int,
	freeFastAck func(*MFastAck), freeSlowAck func(*MLightSlowAck)) *Batcher {
	b := &Batcher{
		fastAcks:      make(chan BatcherOp, size),
		lightSlowAcks: make(chan BatcherOp, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case op := <-b.fastAcks:
				fastAck := op.msg.(*MFastAck)

				fLen := len(b.fastAcks) + 1
				sLen := len(b.lightSlowAcks)
				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				ballot := fastAck.Ballot
				optAcks := &MOptAcks{
					Replica: r.Id,
					Ballot:  fastAck.Ballot,
					Acks: []Ack{Ack{
						CmdId: fastAck.CmdId,
						Dep:   fastAck.Dep,
					}},
				}
				is := map[CommandId]int{fastAck.CmdId: 0}

				acks.FastAcks[0] = *fastAck
				freeFastAck(fastAck)
				for i := 1; i < fLen; i++ {
					opP := <-b.fastAcks
					f := opP.msg.(*MFastAck)
					acks.FastAcks[i] = *f

					if ballot == f.Ballot {
						is[f.CmdId] = len(optAcks.Acks)
						optAcks.Acks = append(optAcks.Acks, Ack{
							CmdId: f.CmdId,
							Dep:   f.Dep,
						})
					} else {
						ballot = -1
					}
					freeFastAck(f)
				}
				for i := 0; i < sLen; i++ {
					opP := <-b.lightSlowAcks
					s := opP.msg.(*MLightSlowAck)
					acks.LightSlowAcks[i] = *s

					if ballot == s.Ballot {
						iCmdId, exists := is[s.CmdId]
						if exists {
							optAcks.Acks[iCmdId].Dep = NilDepOfCmdId(s.CmdId)
						} else {
							is[s.CmdId] = len(optAcks.Acks)
							optAcks.Acks = append(optAcks.Acks, Ack{
								CmdId: s.CmdId,
								Dep:   NilDepOfCmdId(s.CmdId),
							})
						}
					} else {
						ballot = -1
					}
					freeSlowAck(s)
				}

				var (
					m   fastrpc.Serializable
					rpc uint8
				)
				if ballot != -1 {
					m = optAcks
					rpc = r.cs.optAcksRPC
				} else {
					m = acks
					rpc = r.cs.acksRPC
				}
				r.sender.SendToAll(m, rpc)
				if op.sendToClient {
					r.sender.SendToClient(op.cid, m, rpc)
				}

			case op := <-b.lightSlowAcks:
				slowAck := op.msg.(*MLightSlowAck)

				fLen := len(b.fastAcks)
				sLen := len(b.lightSlowAcks) + 1
				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				ballot := slowAck.Ballot
				optAcks := &MOptAcks{
					Replica: r.Id,
					Ballot:  slowAck.Ballot,
					Acks: []Ack{Ack{
						CmdId: slowAck.CmdId,
						Dep:   NilDepOfCmdId(slowAck.CmdId),
					}},
				}
				is := map[CommandId]int{slowAck.CmdId: 0}

				acks.LightSlowAcks[0] = *slowAck
				freeSlowAck(slowAck)
				for i := 1; i < sLen; i++ {
					opP := <-b.lightSlowAcks
					s := opP.msg.(*MLightSlowAck)
					acks.LightSlowAcks[i] = *s

					if ballot == s.Ballot {
						is[s.CmdId] = len(optAcks.Acks)
						optAcks.Acks = append(optAcks.Acks, Ack{
							CmdId: s.CmdId,
							Dep:   NilDepOfCmdId(s.CmdId),
						})
					} else {
						ballot = -1
					}
					freeSlowAck(s)
				}
				for i := 0; i < fLen; i++ {
					opP := <-b.fastAcks
					f := opP.msg.(*MFastAck)
					acks.FastAcks[i] = *f

					if ballot == f.Ballot {
						_, exists := is[f.CmdId]
						if !exists {
							is[f.CmdId] = len(optAcks.Acks)
							optAcks.Acks = append(optAcks.Acks, Ack{
								CmdId: f.CmdId,
								Dep:   f.Dep,
							})
						}
					} else {
						ballot = -1
					}
					freeFastAck(f)
				}

				var (
					m   fastrpc.Serializable
					rpc uint8
				)
				if ballot != -1 {
					m = optAcks
					rpc = r.cs.optAcksRPC
				} else {
					m = acks
					rpc = r.cs.acksRPC
				}
				r.sender.SendToAll(m, rpc)
				if op.sendToClient {
					r.sender.SendToClient(op.cid, m, rpc)
				}
			}
		}
	}()

	return b
}

func (b *Batcher) SendFastAck(f *MFastAck) {
	b.fastAcks <- BatcherOp{
		msg:          f,
		sendToClient: false,
	}
}

func (b *Batcher) SendLightSlowAck(s *MLightSlowAck) {
	b.lightSlowAcks <- BatcherOp{
		msg:          s,
		sendToClient: false,
	}
}

func (b *Batcher) SendFastAckClient(f *MFastAck, cid int32) {
	b.fastAcks <- BatcherOp{
		msg:          f,
		cid:          cid,
		sendToClient: true,
	}
}

func (b *Batcher) SendLightSlowAckClient(s *MLightSlowAck, cid int32) {
	b.lightSlowAcks <- BatcherOp{
		msg:          s,
		cid:          cid,
		sendToClient: true,
	}
}
