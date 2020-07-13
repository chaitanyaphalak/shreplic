package smr

type MsgSetHandler func(interface{}, []interface{})

type MsgSet struct {
	q         Quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}, interface{}) bool
	freeMsg   func(interface{})
	handler   MsgSetHandler
}

func NewMsgSet(q Quorum, accept func(interface{}, interface{}) bool,
	freeMsg func(interface{}), handler MsgSetHandler) *MsgSet {

	return &MsgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		freeMsg:   freeMsg,
		handler:   handler,
	}
}

func (ms *MsgSet) ReinitMsgSet(q Quorum, accept func(interface{}, interface{}) bool,
	freeMsg func(interface{}), handler MsgSetHandler) *MsgSet {

	if ms == nil {
		return NewMsgSet(q, accept, freeMsg, handler)
	}

	ms.q = q
	ms.msgs = []interface{}{}
	ms.leaderMsg = nil
	ms.accept = accept
	ms.freeMsg = freeMsg
	ms.handler = handler
	return ms
}

func (ms *MsgSet) Add(repId int32, isLeader bool, msg interface{}) bool {

	if !ms.q.Contains(repId) {
		return false
	}

	added := false

	if isLeader {
		ms.leaderMsg = msg
		newMsgs := []interface{}{}
		for _, fmsg := range ms.msgs {
			if ms.accept(fmsg, ms.leaderMsg) {
				newMsgs = append(newMsgs, fmsg)
			} else {
				ms.freeMsg(fmsg)
			}
		}
		ms.msgs = newMsgs
		added = true
	} else if ms.accept(msg, ms.leaderMsg) {
		ms.msgs = append(ms.msgs, msg)
		added = true
	} else {
		ms.freeMsg(msg)
	}

	if len(ms.msgs) == len(ms.q) ||
		(len(ms.msgs) == len(ms.q)-1 && ms.leaderMsg != nil) {
		ms.handler(ms.leaderMsg, ms.msgs)
	}

	return added
}

func (ms *MsgSet) Free() {
	if ms.leaderMsg != nil {
		ms.freeMsg(ms.leaderMsg)
	}
	for _, fmsg := range ms.msgs {
		if fmsg != nil {
			ms.freeMsg(fmsg)
		}
	}
}
