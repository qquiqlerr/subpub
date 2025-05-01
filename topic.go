package subpub

import "sync"

// topic represents a single topic in the pub-sub system.
// It contains a map of subscribers and a mutex for thread-safe access.
type topic struct {
	mu          sync.RWMutex
	subscribers map[uint32]*subscriber
}

// newTopic creates a new topic with the given subject.
func newTopic() *topic {
	return &topic{
		subscribers: make(map[uint32]*subscriber),
	}
}

// addSubscriber adds a subscriber to the topic.
func (t *topic) addSubscriber(sub *subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers[sub.id] = sub
}

// removeSubscriber removes a subscriber from the topic.
func (t *topic) removeSubscriber(id uint32) {
	t.mu.Lock()
	sub, exists := t.subscribers[id]
	if exists {
		delete(t.subscribers, id)
	}
	t.mu.Unlock()

	if exists {
		close(sub.ch)
		<-sub.done
	}
}

// publish sends a message to all subscribers of the topic.
func (t *topic) publish(msg interface{}) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, sub := range t.subscribers {
		// Send the message to the subscriber's channel.
		// If the channel is full, it will block until space is available.
		// It helps to save FIFOs in the subscriber's channel.

		sub.ch <- msg
	}
}
