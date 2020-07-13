package paxoi

type Batcher struct {
	fastAcks      chan *MFastAck
	lightSlowAcks chan *MLightSlowAck
}

func NewBatcher(r *Replica, size int,
	freeFastAck func(*MFastAck), freeSlowAck func(*MLightSlowAck)) *Batcher {
	b := &Batcher{
		fastAcks:      make(chan *MFastAck, size),
		lightSlowAcks: make(chan *MLightSlowAck, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case fastAck := <-b.fastAcks:
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
					f := <-b.fastAcks
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
					s := <-b.lightSlowAcks
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

				if ballot != -1 {
					r.sender.SendToAll(optAcks, r.cs.optAcksRPC)
				} else {
					r.sender.SendToAll(acks, r.cs.acksRPC)
				}

			case slowAck := <-b.lightSlowAcks:
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
					s := <-b.lightSlowAcks
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
					f := <-b.fastAcks
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

				if ballot != -1 {
					r.sender.SendToAll(optAcks, r.cs.optAcksRPC)
				} else {
					r.sender.SendToAll(acks, r.cs.acksRPC)
				}
			}
		}
	}()

	return b
}

func (b *Batcher) SendFastAck(f *MFastAck) {
	b.fastAcks <- f
}

func (b *Batcher) SendLightSlowAck(s *MLightSlowAck) {
	b.lightSlowAcks <- s
}
