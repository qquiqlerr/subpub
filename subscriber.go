package subpub

// subscriber represents a single subscriber to a topic.
// It contains a unique ID, the subject it subscribes to,
// a channel for receiving messages, and a handler function to process those messages.
// The subscriber runs in a separate goroutine to handle incoming messages asynchronously.
type subscriber struct {
	id      uint32
	subject string
	ch      chan interface{}
	done    chan struct{}
	handler MessageHandler
}

// NewSubscriber creates a new subscriber instance.
// bufferSize specifies the size of the channel buffer to avoid blocking the publisher.
func newSubscriber(id uint32, subject string, handler MessageHandler, bufferSize int) *subscriber {
	s := &subscriber{
		id:      id,
		subject: subject,
		ch:      make(chan interface{}, bufferSize),
		done:    make(chan struct{}),
		handler: handler,
	}
	go s.run()
	return s
}

// run starts the subscriber's message processing loop.
func (sub *subscriber) run() {
	for msg := range sub.ch {
		sub.handler(msg)
	}
	close(sub.done)
}
