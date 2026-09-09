// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Tests provide 100% coverage of ringbuffer.go.

// --- NewRingBuffer ---

func TestNewRingBuffer_ValidCapacity(t *testing.T) {
	t.Parallel()
	rb, err := NewRingBuffer[int](10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rb == nil {
		t.Fatal("expected non-nil RingBuffer")
	}
}

func TestNewRingBuffer_ZeroCapacity(t *testing.T) {
	t.Parallel()
	_, err := NewRingBuffer[int](0)
	if err == nil {
		t.Fatal("expected error for zero capacity")
	}
}

// --- Push / Pop ---

func TestPushPop_SingleSubscriber(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	v := 42
	rb.Push(&v)

	got, _, err := sub.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != 42 {
		t.Fatalf("expected 42, got %d", *got)
	}
}

func TestPushPop_Order(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](8)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 5 {
		v := i
		rb.Push(&v)
	}

	for i := range 5 {
		got, _, err := sub.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if *got != i {
			t.Fatalf("expected %d, got %d", i, *got)
		}
	}
}

func TestPushPop_MultipleSubscribers(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](8)
	s1 := rb.NewSubscriber()
	s2 := rb.NewSubscriber()
	defer s1.Close()
	defer s2.Close()

	v := 99
	rb.Push(&v)

	got1, _, err := s1.Pop(context.Background())
	if err != nil || *got1 != 99 {
		t.Fatalf("s1: expected 99, got %v, err %v", got1, err)
	}

	got2, _, err := s2.Pop(context.Background())
	if err != nil || *got2 != 99 {
		t.Fatalf("s2: expected 99, got %v, err %v", got2, err)
	}
}

func TestPop_BlocksUntilPush(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	v := 7
	go func() {
		time.Sleep(50 * time.Millisecond)
		rb.Push(&v)
	}()

	got, _, err := sub.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != 7 {
		t.Fatalf("expected 7, got %d", *got)
	}
}

// --- Sequence number ---

func TestSeq_StartsAtZero(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	v := 1
	rb.Push(&v)

	_, seq, err := sub.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seq != 0 {
		t.Fatalf("expected seq 0 on first pop, got %d", seq)
	}
}

func TestSeq_IncrementsOnEachPop(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](8)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 5 {
		v := i
		rb.Push(&v)
	}

	for i := range 5 {
		_, seq, err := sub.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if seq != uint64(i) {
			t.Fatalf("expected seq %d, got %d", i, seq)
		}
	}
}

func TestSeq_IndependentPerSubscriber(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](8)
	s1 := rb.NewSubscriber()
	s2 := rb.NewSubscriber()
	defer s1.Close()
	defer s2.Close()

	for i := range 3 {
		v := i
		rb.Push(&v)
	}

	for i := range 3 {
		_, seq, err := s1.Pop(context.Background())
		if err != nil {
			t.Fatalf("s1 unexpected error: %v", err)
		}
		if seq != uint64(i) {
			t.Fatalf("s1 expected seq %d, got %d", i, seq)
		}
	}

	_, seq, err := s2.Pop(context.Background())
	if err != nil {
		t.Fatalf("s2 unexpected error: %v", err)
	}
	if seq != 0 {
		t.Fatalf("s2 expected seq 0, got %d", seq)
	}
}

func TestSeq_ReflectsGlobalPushPosition(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 4 {
		v := i
		rb.Push(&v)
	}

	_, seq, err := sub.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := uint64(1)
	if seq != expected {
		t.Fatalf("expected seq %d after skip, got %d", expected, seq)
	}
}

// --- Context cancellation ---

func TestPop_ContextCancelledBeforeCall(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := sub.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestPop_ContextCancelledWhileBlocking(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, _, err := sub.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
}

func TestPop_ContextTimeout(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := sub.Pop(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// --- Close ---

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()

	sub.Close()
	sub.Close()
}

func TestClose_PopAfterClose(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	sub.Close()

	_, _, err := sub.Pop(context.Background())
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

// --- Skipped ---

func TestSkipped_InitiallyZero(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	if sub.Skipped() != 0 {
		t.Fatalf("expected 0 skipped, got %d", sub.Skipped())
	}
}

func TestSkipped_IncrementedWhenLapped(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 4 {
		v := i
		rb.Push(&v)
	}

	if sub.Skipped() == 0 {
		t.Fatal("expected skipped > 0 after being lapped")
	}
}

func TestSkipped_NotIncrementedWhenKeepingUp(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 3 {
		v := i
		rb.Push(&v)
		_, _, err := sub.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if sub.Skipped() != 0 {
		t.Fatalf("expected 0 skipped, got %d", sub.Skipped())
	}
}

func TestSkipped_IndependentPerSubscriber(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	slow := rb.NewSubscriber()
	fast := rb.NewSubscriber()
	defer slow.Close()
	defer fast.Close()

	for i := range 4 {
		v := i
		_ = rb.Push(&v)
		_, _, _ = fast.Pop(context.Background())
	}

	if slow.Skipped() == 0 {
		t.Fatal("expected slow subscriber to have skipped > 0")
	}
	if fast.Skipped() != 0 {
		t.Fatalf("expected fast subscriber to have 0 skipped, got %d", fast.Skipped())
	}
}

// --- Concurrency ---

func TestConcurrent_PushPop(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](128)
	const n = 100

	var wg sync.WaitGroup
	results := make([]int, 0, n)
	var mu sync.Mutex

	sub := rb.NewSubscriber()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer sub.Close()
		for range n {
			v, _, err := sub.Pop(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			results = append(results, *v)
			mu.Unlock()
		}
	}()

	for i := range n {
		v := i
		rb.Push(&v)
	}

	wg.Wait()

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestConcurrent_PushPop_WithSkips(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](16)
	const n = 100

	var wg sync.WaitGroup
	sub := rb.NewSubscriber()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer sub.Close()
		received := 0
		for uint64(received)+sub.Skipped() < n {
			_, _, err := sub.Pop(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			received++
		}
		if uint64(received)+sub.Skipped() != n {
			t.Errorf("expected received+skipped=%d, got %d", n, uint64(received)+sub.Skipped())
		}
	}()

	for i := range n {
		v := i
		rb.Push(&v)
	}

	wg.Wait()
}

func TestWrapAround_CorrectValues(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	const n = 12
	for i := range n {
		v := i
		rb.Push(&v)
		got, _, err := sub.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if *got != i {
			t.Fatalf("expected %d, got %d", i, *got)
		}
	}
}

func TestConcurrent_MultipleSubscribers_WithSkips(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](16)
	const n = 100

	var wg sync.WaitGroup
	for range 3 {
		sub := rb.NewSubscriber()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sub.Close()
			received := 0
			for uint64(received)+sub.Skipped() < n {
				_, _, err := sub.Pop(context.Background())
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				received++
			}
			if uint64(received)+sub.Skipped() != n {
				t.Errorf("expected received+skipped=%d, got %d", n, uint64(received)+sub.Skipped())
			}
		}()
	}

	for i := range n {
		v := i
		rb.Push(&v)
	}
	wg.Wait()
}

func TestNewSubscriber_MidStream(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](8)

	for i := range 3 {
		v := i
		rb.Push(&v)
	}

	sub := rb.NewSubscriber()
	defer sub.Close()

	v := 99
	rb.Push(&v)

	got, _, err := sub.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != 99 {
		t.Fatalf("expected 99 (element pushed after subscriber creation), got %d", *got)
	}
}

func TestPush_SkipCountReturnValue(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	s1 := rb.NewSubscriber()
	s2 := rb.NewSubscriber()
	defer s1.Close()
	defer s2.Close()

	skipped := 0
	for i := range 4 {
		v := i
		skipped += rb.Push(&v)
	}

	if skipped < 2 {
		t.Fatalf("expected at least 2 total skips across subscribers, got %d", skipped)
	}
}

func TestSkipped_AccumulatesAcrossMultipleLaps(t *testing.T) {
	t.Parallel()
	rb, _ := NewRingBuffer[int](4)
	sub := rb.NewSubscriber()
	defer sub.Close()

	for i := range 12 {
		v := i
		rb.Push(&v)
	}

	if sub.Skipped() < 2 {
		t.Fatalf("expected skipped >= 2 after multiple laps, got %d", sub.Skipped())
	}
}
