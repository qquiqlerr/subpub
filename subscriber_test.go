package subpub

import (
	"context"
	"github.com/stretchr/testify/assert"
	"runtime"
	"sync"
	"testing"
)

func TestSubscriber_run_FIFOOrder(t *testing.T) {
	subPub := NewSubPub()
	receivedMsgs := make([]interface{}, 0)
	wg := sync.WaitGroup{}

	msgs := []interface{}{"msg1", "msg2", "msg3"}

	sub, err := subPub.Subscribe("test.topic", func(msg interface{}) {
		defer wg.Done()
		receivedMsgs = append(receivedMsgs, msg)
	})
	assert.NoError(t, err)

	// Publish messages
	for _, msg := range msgs {
		wg.Add(1)
		err = subPub.Publish("test.topic", msg)
		assert.NoError(t, err)
	}

	// Wait for all messages to be processed
	wg.Wait()

	// Check if the received messages are in the same order as published
	assert.Equal(t, msgs, receivedMsgs)

	sub.Unsubscribe()
	assert.NoError(t, subPub.Close(context.Background()))
}

func TestSubscriber_run_GoroutineLeak(t *testing.T) {
	subPub := NewSubPub()

	// Subscribe to a topic
	sub, err := subPub.Subscribe("test.topic", func(msg interface{}) {})
	assert.NoError(t, err)

	gCnt := runtime.NumGoroutine()

	// Unsubscribe
	sub.Unsubscribe()

	// Check if the goroutine count has decreased
	assert.Equal(t, gCnt-1, runtime.NumGoroutine())
}
