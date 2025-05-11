package subpub

import (
	"context"
	"errors"
	"sync"
)

var ErrClosed = errors.New("subpub: already closed")

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
}

func NewSubPub() SubPub {
	return &subpub{
		subs: make(map[string]map[*subscription]struct{}),
	}
}

type subpub struct {
	mu     sync.RWMutex
	subs   map[string]map[*subscription]struct{}
	closed bool
	wg     sync.WaitGroup
}

type subscription struct {
	subject string
	cb      MessageHandler

	queue []interface{}
	qmu   sync.Mutex
	qcond *sync.Cond

	msgCh chan interface{}

	closed bool
	once   sync.Once
	parent *subpub
}

func (sp *subpub) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed {
		return nil, ErrClosed
	}
	ss := &subscription{
		subject: subject,
		cb:      cb,
		queue:   make([]interface{}, 0),
		msgCh:   make(chan interface{}),
		parent:  sp,
	}
	ss.qcond = sync.NewCond(&ss.qmu)

	if sp.subs[subject] == nil {
		sp.subs[subject] = make(map[*subscription]struct{})
	}
	sp.subs[subject][ss] = struct{}{}

	sp.wg.Add(1)
	go ss.dispatch()
	go ss.handle()

	return ss, nil
}

func (sp *subpub) Publish(subject string, msg interface{}) error {
	sp.mu.RLock()
	if sp.closed {
		return ErrClosed
	}
	subs := sp.subs[subject]
	var list []*subscription
	for ss := range subs {
		list = append(list, ss)
	}
	sp.mu.RUnlock()

	for _, ss := range list {
		ss.enqueue(msg)
	}
	return nil
}

func (sp *subpub) Close(ctx context.Context) error {
	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return nil
	}
	sp.closed = true
	for _, subs := range sp.subs {
		for ss := range subs {
			ss.unsubscribeInternal()
		}
	}
	sp.subs = nil
	sp.mu.Unlock()

	done := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ss *subscription) enqueue(msg interface{}) {
	ss.qmu.Lock()
	if ss.closed {
		ss.qmu.Unlock()
		return
	}
	ss.queue = append(ss.queue, msg)
	ss.qcond.Signal()
	ss.qmu.Unlock()
}

func (ss *subscription) dispatch() {
	defer ss.parent.wg.Done()
	for {
		ss.qmu.Lock()
		for len(ss.queue) == 0 && !ss.closed {
			ss.qcond.Wait()
		}
		if ss.closed && len(ss.queue) == 0 {
			ss.qmu.Unlock()
			break
		}
		msg := ss.queue[0]
		ss.queue = ss.queue[1:]
		ss.qmu.Unlock()

		ss.msgCh <- msg
	}
	close(ss.msgCh)
}

func (ss *subscription) handle() {
	for msg := range ss.msgCh {
		ss.cb(msg)
	}
}

func (ss *subscription) unsubscribeInternal() {
	ss.once.Do(func() {
		ss.qmu.Lock()
		ss.closed = true
		ss.queue = nil
		ss.qmu.Unlock()
		ss.qcond.Broadcast()
	})
}

func (ss *subscription) Unsubscribe() {
	sp := ss.parent
	sp.mu.Lock()
	if subs, ok := sp.subs[ss.subject]; ok {
		delete(subs, ss)
	}
	sp.mu.Unlock()
	ss.unsubscribeInternal()
}
