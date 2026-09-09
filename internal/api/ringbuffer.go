// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// All methods on subscriber must be called from the same goroutine.
type subscriber[T any] struct {
	rb           *RingBuffer[T]
	tail         uint64
	seq          uint64
	totalSkipped atomic.Uint64
}

// Pop returns the next element and a monotonically increasing sequence number
// that reflects any skipped elements, allowing clients to detect gaps in the
// FIE stream. Blocks until an element is available or the context is canceled.
func (sub *subscriber[T]) Pop(ctx context.Context) (*T, uint64, error) {
	// Safe because Pop and Close must be called from the same goroutine.
	if sub.rb == nil {
		return nil, 0, fmt.Errorf("subscriber already closed")
	}
	if ctx.Err() != nil {
		return nil, 0, ctx.Err()
	}
	// Wake waiting goroutines if this subscriber's context is canceled.
	cond := sub.rb.cond
	stop := context.AfterFunc(ctx, func() {
		cond.Broadcast()
	})
	defer stop()
	sub.rb.mutex.Lock()
	defer sub.rb.mutex.Unlock()
	for sub.tail == sub.rb.head {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		// Atomically release the lock and sleep. Reacquires the lock when woken.
		sub.rb.cond.Wait()
	}
	e := sub.rb.buffer[sub.tail]
	sub.tail = (sub.tail + 1) % sub.rb.capacity
	sub.seq++
	return e, sub.seq - 1 + sub.Skipped(), nil
}

// Close releases the subscriber. After Close, the subscriber's tail is no longer
// tracked by Push, so slow subscriber skipping will not apply to it.
// Calling Close multiple times is a no-op.
func (sub *subscriber[T]) Close() {
	if sub.rb == nil {
		return
	}
	sub.rb.mutex.Lock()
	defer sub.rb.mutex.Unlock()
	if _, ok := sub.rb.subscribers[sub]; ok {
		delete(sub.rb.subscribers, sub)
		sub.rb = nil
	}
}

// Skipped returns the number of elements that were overwritten before
// this subscriber could read them, indicating the subscriber is falling
// behind the producer.
func (sub *subscriber[T]) Skipped() uint64 {
	return sub.totalSkipped.Load()
}

// RingBuffer is a fixed-capacity circular buffer with multiple independent
// subscribers. Slow subscribers are lapped automatically rather than blocking
// the producer.
//
// Push is safe to call concurrently with Pop and Close, but is intended
// to be called from a single goroutine.
type RingBuffer[T any] struct {
	mutex       *sync.Mutex
	cond        *sync.Cond
	head        uint64
	buffer      []*T
	capacity    uint64
	subscribers map[*subscriber[T]]struct{}
}

// NewRingBuffer creates a new RingBuffer with the given capacity.
func NewRingBuffer[T any](capacity int) (*RingBuffer[T], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("invalid argument: capacity must be greater than zero")
	}
	mu := &sync.Mutex{}
	return &RingBuffer[T]{
		mutex:       mu,
		buffer:      make([]*T, capacity),
		capacity:    uint64(capacity),
		subscribers: make(map[*subscriber[T]]struct{}),
		cond:        sync.NewCond(mu),
	}, nil
}

// NewSubscriber creates a new subscriber starting at the current head position.
// The subscriber will only receive elements pushed after its creation.
func (rb *RingBuffer[T]) NewSubscriber() *subscriber[T] {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()
	sub := &subscriber[T]{
		tail: rb.head,
		rb:   rb,
	}
	rb.subscribers[sub] = struct{}{}
	return sub
}

// Push adds a new element to the ring buffer and wakes all waiting subscribers.
// Returns the number of slow subscribers whose tails were skipped.
func (rb *RingBuffer[T]) Push(e *T) int {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()
	rb.buffer[rb.head] = e
	rb.head = (rb.head + 1) % rb.capacity
	// Advance the tails of slow subscribers that have been lapped.
	skipped := 0
	for sub := range rb.subscribers {
		if rb.head == sub.tail {
			skipped++
			sub.tail = (sub.tail + 1) % rb.capacity
			sub.totalSkipped.Add(1)
		}
	}
	// Broadcast after all state is consistent.
	rb.cond.Broadcast()
	return skipped
}
