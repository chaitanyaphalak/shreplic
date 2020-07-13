package paxoi

import (
	"github.com/vonaka/shreplic/server/smr"
	"github.com/vonaka/shreplic/state"
)

type replyArgs struct {
	val     state.Value
	propose *smr.GPropose
	finish  chan interface{}
	cmdId   CommandId
}

type replyChan struct {
	args chan *replyArgs
	rep  *smr.ProposeReplyTS
}

func NewReplyChan(r *smr.Replica) *replyChan {
	rc := &replyChan{
		args: make(chan *replyArgs, smr.CHAN_BUFFER_SIZE),
		rep: &smr.ProposeReplyTS{
			OK: smr.TRUE,
		},
	}

	go func() {
		slot := 0
		for !r.Shutdown {
			args := <-rc.args

			if args.propose.Collocated {
				rc.rep.CommandId = args.propose.CommandId
				rc.rep.Value = args.val
				rc.rep.Timestamp = args.propose.Timestamp

				r.ReplyProposeTS(rc.rep, args.propose.Reply, args.propose.Mutex)
			}

			args.finish <- slot
			slot = (slot + 1) % HISTORY_SIZE
		}
	}()

	return rc
}

func (r *replyChan) reply(desc *commandDesc, cmdId CommandId, val state.Value) {
	r.args <- &replyArgs{
		val:     val,
		propose: desc.propose,
		cmdId:   cmdId,
		finish:  desc.msgs,
	}
}
