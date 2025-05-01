package subpub

import "sync"

// subscriptionImpl is the implementation of the Subscription interface.
// It contains a reference to the topic and a unique subscriber ID.
type subscriptionImpl struct {
	topic        *topic
	subscriberId uint32
	once         sync.Once
}

// NewSubscription creates a new subscription instance.
func NewSubscription(topic *topic, subscriberId uint32) Subscription {
	return &subscriptionImpl{
		topic:        topic,
		subscriberId: subscriberId,
	}
}

// Unsubscribe removes the subscription from the topic.
// It uses a sync.Once to ensure that the unsubscribe operation is performed only once.
func (s *subscriptionImpl) Unsubscribe() {
	s.once.Do(func() {
		s.topic.removeSubscriber(s.subscriberId)
	})
}
