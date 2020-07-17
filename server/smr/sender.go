package smr

import "github.com/vonaka/shreplic/tools/fastrpc"

const (
	SEND_ALL = iota
	SEND_QUORUM
	SEND_EXCEPT
	SEND_SINGLE
	SEND_CLIENT
)

const ARGS_NUM int = CHAN_BUFFER_SIZE

type SendArg struct {
	msg      fastrpc.Serializable
	rpc      uint8
	quorum   Quorum
	sendType int32
	clientId int32
	free     func()
}

type Sender chan SendArg

func NewSender(r *Replica) Sender {
	s := Sender(make(chan SendArg, ARGS_NUM))

	go func() {
		for !r.Shutdown {
			arg := <-s
			switch arg.sendType {
			case SEND_ALL:
				sendToAll(r, arg.msg, arg.rpc)
			case SEND_QUORUM:
				sendToQuorum(r, arg.quorum, arg.msg, arg.rpc)
			case SEND_EXCEPT:
				sendExcept(r, arg.quorum, arg.msg, arg.rpc)
			case SEND_CLIENT:
				r.SendClientMsg(arg.clientId, arg.rpc, arg.msg)
			case SEND_SINGLE:
			}
			if arg.free != nil {
				arg.free()
			}
		}
	}()

	return s
}

func (s Sender) SendToAllAndFree(msg fastrpc.Serializable,
	rpc uint8, free func()) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		sendType: SEND_ALL,
		free:     free,
	}
}

func (s Sender) SendToQuorumAndFree(q Quorum,
	msg fastrpc.Serializable, rpc uint8, free func()) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		quorum:   q,
		sendType: SEND_QUORUM,
		free:     free,
	}
}

func (s Sender) SendExceptAndFree(q Quorum,
	msg fastrpc.Serializable, rpc uint8, free func()) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		quorum:   q,
		sendType: SEND_EXCEPT,
		free:     free,
	}
}

func (s Sender) SendToClientAndFree(cid int32,
	msg fastrpc.Serializable, rpc uint8, free func()) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		clientId: cid,
		sendType: SEND_CLIENT,
		free:     free,
	}
}

func (s Sender) SendToAll(msg fastrpc.Serializable, rpc uint8) {
	s.SendToAllAndFree(msg, rpc, nil)
}

func (s Sender) SendToQuorum(q Quorum, msg fastrpc.Serializable, rpc uint8) {
	s.SendToQuorumAndFree(q, msg, rpc, nil)
}

func (s Sender) SendExcept(q Quorum, msg fastrpc.Serializable, rpc uint8) {
	s.SendExceptAndFree(q, msg, rpc, nil)
}

func (s Sender) SendToClient(cid int32, msg fastrpc.Serializable, rpc uint8) {
	s.SendToClientAndFree(cid, msg, rpc, nil)
}

func sendToAll(r *Replica, msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		r.M.Lock()
		if r.Alive[p] {
			r.M.Unlock()
			r.SendMsg(p, rpc, msg)
			r.M.Lock()
		}
		r.M.Unlock()
	}
}

func sendToQuorum(r *Replica, q Quorum,
	msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		if !q.Contains(p) {
			continue
		}
		r.M.Lock()
		if r.Alive[p] {
			r.M.Unlock()
			r.SendMsg(p, rpc, msg)
			r.M.Lock()
		}
		r.M.Unlock()
	}
}

func sendExcept(r *Replica, q Quorum,
	msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		if q.Contains(p) {
			continue
		}
		r.M.Lock()
		if r.Alive[p] {
			r.M.Unlock()
			r.SendMsg(p, rpc, msg)
			r.M.Lock()
		}
		r.M.Unlock()
	}
}
