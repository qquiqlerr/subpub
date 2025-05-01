package subpub

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSubscriptionImpl_Unsubscribe_Once(t *testing.T) {
	subPub := NewSubPub()
	sub, err := subPub.Subscribe("test.topic", func(msg interface{}) {})
	assert.NoError(t, err)

	sub.Unsubscribe()
	assert.NotPanics(t, func() {
		sub.Unsubscribe() // Call Unsubscribe again to check for idempotency
	})
	assert.NoError(t, subPub.Close(context.Background()))
}

func TestSubscriptionImpl_Unsubscribe(t *testing.T) {
	// Create a new PubSub instance
	subPub := NewSubPub()

	receivedMsg := make(chan interface{}, 1)

	sub, err := subPub.Subscribe("test.topic", func(msg interface{}) {
		receivedMsg <- msg
	})
	assert.NoError(t, err)

	// Publish a message
	err = subPub.Publish("test.topic", "test message")
	assert.NoError(t, err)

	select {
	case msg := <-receivedMsg:
		assert.Equal(t, "test message", msg)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for message")
	}

	// Unsubscribe
	sub.Unsubscribe()

	// Publish another message
	err = subPub.Publish("test.topic", "another message")
	assert.NoError(t, err)

	select {
	case msg := <-receivedMsg:
		t.Fatal("Received message after unsubscribe:", msg)
	case <-time.After(100 * time.Millisecond):
	}

	// Close the PubSub instance
	assert.NoError(t, subPub.Close(context.Background()))
}
