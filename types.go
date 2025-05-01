package subpub

import "context"

// MessageHandler is a callback function that processes messages delivered to subscribers.
type MessageHandler func(msg interface{})

// Subscription represents a subscription to a subject.
type Subscription interface {
	// Unsubscribe removes interest in the current subject.
	Unsubscribe()
}

// SubPub is the main interface of the pub-sub system.
type SubPub interface {
	// Subscribe creates an asynchronous queue subscriber on the given subject.
	Subscribe(subject string, cb MessageHandler) (Subscription, error)

	// Publish sends a message to all subscribers of the subject.
	Publish(subject string, msg interface{}) error

	// Close gracefully shuts down the pub-sub system.
	Close(ctx context.Context) error
}
