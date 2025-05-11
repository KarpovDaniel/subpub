package subpub

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSubscribeAndPublish(t *testing.T) {
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	sub, err := sp.Subscribe("news", func(msg interface{}) {
		defer wg.Done()
		if msg != "hello" {
			t.Errorf("expected 'hello', got %v", msg)
		}
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	err = sp.Publish("news", "hello")
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	wg.Wait()
	sub.Unsubscribe()
}

func TestMessageOrder(t *testing.T) {
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var mu sync.Mutex
	var received []int

	done := make(chan struct{})

	_, err := sp.Subscribe("numbers", func(msg interface{}) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, msg.(int))
		if len(received) == 5 {
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_ = sp.Publish("numbers", i)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for messages")
	}

	mu.Lock()
	defer mu.Unlock()
	for i, v := range received {
		if v != i+1 {
			t.Errorf("expected %d, got %d", i+1, v)
		}
	}
}

func TestMultipleSubscribers(t *testing.T) {
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)

	cb := func(expected string) MessageHandler {
		return func(msg interface{}) {
			defer wg.Done()
			if msg != expected {
				t.Errorf("expected %v, got %v", expected, msg)
			}
		}
	}

	_, _ = sp.Subscribe("event", cb("data"))
	_, _ = sp.Subscribe("event", cb("data"))

	_ = sp.Publish("event", "data")

	wg.Wait()
}

func TestSlowSubscriberDoesNotBlock(t *testing.T) {
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var fastReceived bool
	var wg sync.WaitGroup
	wg.Add(2)

	_, _ = sp.Subscribe("topic", func(msg interface{}) {
		time.Sleep(500 * time.Millisecond)
		wg.Done()
	})

	_, _ = sp.Subscribe("topic", func(msg interface{}) {
		fastReceived = true
		wg.Done()
	})

	_ = sp.Publish("topic", "message")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: slow subscriber blocked others")
	}

	if !fastReceived {
		t.Error("fast subscriber did not receive message")
	}
}

func TestUnsubscribe(t *testing.T) {
	sp := NewSubPub()
	defer sp.Close(context.Background())

	var mu sync.Mutex
	var count int

	sub, _ := sp.Subscribe("key", func(msg interface{}) {
		mu.Lock()
		defer mu.Unlock()
		count++
	})

	_ = sp.Publish("key", "msg1")
	time.Sleep(100 * time.Millisecond)

	sub.Unsubscribe()
	_ = sp.Publish("key", "msg2")
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 message, got %d", count)
	}
}

func TestCloseWithContext(t *testing.T) {
	sp := NewSubPub()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = sp.Close(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Close did not return on time")
	}
}
