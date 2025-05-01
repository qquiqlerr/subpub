package subpub

import (
	"context"
	"sync"
	"sync/atomic"
)

// subPubImpl is the implementation of the SubPub interface.
// It manages topics and subscribers, allowing for message publishing and subscription.
// It uses a mutex to ensure thread-safe access to shared resources.
type subPubImpl struct {
	mu     sync.RWMutex
	topics map[string]*topic
	closed bool
	idGen  uint32
}

// Subscribe creates a new subscription to a given subject.
// It returns a Subscription object that can be used to unsubscribe from the topic.
func (s *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {

	if s.closed {
		return nil, ErrSubPubClosed
	}
	s.mu.RLock()
	t, exists := s.topics[subject]
	s.mu.RUnlock()

	if !exists {
		t = newTopic()
		s.topics[subject] = t
	}
	newId := atomic.AddUint32(&s.idGen, 1)
	sub := newSubscriber(newId, subject, cb, 64)

	t.addSubscriber(sub)

	return NewSubscription(t, sub.id), nil
}

// Publish sends a message to all subscribers of the given subject.
func (s *subPubImpl) Publish(subject string, msg interface{}) error {

	if s.closed {
		return ErrSubPubClosed
	}
	s.mu.RLock()
	t, exists := s.topics[subject]
	s.mu.RUnlock()
	if !exists {
		return ErrTopicNotFound
	}

	t.publish(msg)
	return nil
}

// Close gracefully shuts down the pub-sub system.
func (s *subPubImpl) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var wg sync.WaitGroup
	for _, t := range s.topics {
		wg.Add(1)
		go func(t *topic) {
			defer wg.Done()
			t.mu.Lock()
			defer t.mu.Unlock()
			for _, sub := range t.subscribers {
				t.removeSubscriber(sub.id)
			}
		}(t)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func NewSubPub() SubPub {
	return &subPubImpl{
		mu:     sync.RWMutex{},
		topics: make(map[string]*topic),
		closed: false,
		idGen:  0,
	}
}
