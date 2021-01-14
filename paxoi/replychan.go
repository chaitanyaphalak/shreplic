package paxoi

import (
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type replyArgs struct {
	dep     Dep
	val     state.Value
	cmdId   CommandId
	finish  chan interface{}
	propose *smr.GPropose
}

type replyChan struct {
	rep      *smr.ProposeReplyTS
	ok       chan struct{}
	exit     chan struct{}
	args     chan *replyArgs
	readArgs chan *replyArgs
}

func NewReplyChan(r *Replica) *replyChan {
	rc := &replyChan{
		rep: &smr.ProposeReplyTS{
			OK: smr.TRUE,
		},
		ok:       make(chan struct{}, 1),
		exit:     make(chan struct{}, 2),
		args:     make(chan *replyArgs, smr.CHAN_BUFFER_SIZE),
		readArgs: make(chan *replyArgs, smr.CHAN_BUFFER_SIZE),
	}

	go func() {
		for !r.Shutdown {
			select {
			case <-rc.exit:
				rc.ok <- struct{}{}
				return
			case args := <-rc.args:
				if args.propose.Collocated && !r.optExec {
					rc.rep.CommandId = args.propose.CommandId
					rc.rep.Value = args.val
					rc.rep.Timestamp = args.propose.Timestamp
					r.ReplyProposeTS(rc.rep, args.propose.Reply, args.propose.Mutex)
				} else if r.optExec && r.Id == r.leader() {
					reply := &MReply{
						Replica: r.Id,
						Ballot:  r.ballot,
						CmdId:   args.cmdId,
						Dep:     args.dep,
						Rep:     args.val,
					}
					r.sender.SendToClient(args.propose.ClientId, reply, r.cs.replyRPC)
				}
				r.historySize = (r.historySize % HISTORY_SIZE) + 1
				args.finish <- (r.historySize - 1)
				r.gc.Prepare(r, args.cmdId)

			case args := <-rc.readArgs:
				reply := &MReadReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   args.cmdId,
					Rep:     args.val,
				}
				r.sender.SendToClient(args.propose.ClientId, reply, r.cs.readReplyRPC)
			}
		}
	}()

	return rc
}

func (r *replyChan) stop() {
	r.exit <- struct{}{}
	<-r.ok
}

func (r *replyChan) reply(desc *commandDesc, cmdId CommandId, val state.Value) {
	dep := make([]CommandId, len(desc.dep))
	copy(dep, desc.dep)
	r.args <- &replyArgs{
		dep:     dep,
		val:     val,
		cmdId:   cmdId,
		finish:  desc.msgs,
		propose: desc.propose,
	}
}

func (r *replyChan) readReply(p *smr.GPropose, cmdId CommandId, val state.Value) {
	r.readArgs <- &replyArgs{
		val:     val,
		cmdId:   cmdId,
		propose: p,
	}
}
