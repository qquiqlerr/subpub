package subpub

import "errors"

var (
	// ErrSubPubClosed errors when the subpub system is closed and no longer accepting subscriptions or messages.
	ErrSubPubClosed = errors.New("subpub is closed")
	// ErrTopicNotFound errors when a topic is not found in the subpub system.
	ErrTopicNotFound = errors.New("topic not found")
)
