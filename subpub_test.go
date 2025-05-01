package subpub

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_SubPub_SlowNotBlock(t *testing.T) {
	// Create a new PubSub instance
	subPub := NewSubPub()

	receivedMsgSlow := make(chan interface{}, 1)
	receivedMsgFast := make(chan interface{}, 1)

	wait := make(chan struct{})

	slowSub, err := subPub.Subscribe("test.topic", func(msg interface{}) {
		<-wait // Wait for the main goroutine to signal before processing
		receivedMsgSlow <- msg
	})
	assert.NoError(t, err)

	fastSub, err := subPub.Subscribe("test.topic", func(msg interface{}) {
		receivedMsgFast <- msg
	})
	assert.NoError(t, err)

	// Publish a message
	err = subPub.Publish("test.topic", "test message")
	assert.NoError(t, err)

	select {
	case msg := <-receivedMsgSlow:
		t.Fatal("Received message in slow subscriber before wait:", msg)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case msg := <-receivedMsgFast:
		assert.Equal(t, "test message", msg)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for message in fast subscriber")
	}

	// Signal the slow subscriber to process the message
	close(wait)
	select {
	case msg := <-receivedMsgSlow:
		assert.Equal(t, "test message", msg)
	case <-time.After(100 * time.Millisecond):
	}

	// Unsubscribe
	slowSub.Unsubscribe()
	fastSub.Unsubscribe()

	assert.NoError(t, subPub.Close(context.Background()))
}

func Test_SubPub_NoTopic(t *testing.T) {
	subPub := NewSubPub()

	// Attempt to publish to a non-existent topic
	err := subPub.Publish("nonexistent.topic", "test message")
	assert.Equal(t, ErrTopicNotFound, err)

	// Attempt to subscribe to a non-existent topic
	sub, err := subPub.Subscribe("nonexistent.topic", func(msg interface{}) {})
	assert.NoError(t, err)

	// Attempt to publish to a non-existent topic again(after subscription topic is created)
	err = subPub.Publish("nonexistent.topic", "test message")
	assert.NoError(t, err)

	// Unsubscribe and close
	sub.Unsubscribe()
	assert.NoError(t, subPub.Close(context.Background()))
}
