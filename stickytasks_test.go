package stickytasks

import (
	"runtime"
	"testing"
	"time"
)

func TestStickiness(t *testing.T) {
	time.Sleep(1) // Start the timer goroutine

	ch := make(chan int)
	start := runtime.NumGoroutine()
	workers := New(0)
	workers.Do("a", newResponder(ch, 1, 10*time.Millisecond).respond)
	workers.Do("a", newResponder(ch, 3, 15*time.Millisecond).respond)
	workers.Do("b", newResponder(ch, 2, 20*time.Millisecond).respond)
	for i := 1; i <= 3; i++ {
		v := <-ch
		if i != v {
			t.Errorf("Received %d, want %d\n", v, i)
		}
	}

	workers.Shutdown()
	time.Sleep(10 * time.Millisecond)
	leak := runtime.NumGoroutine() - start
	if leak > 0 {
		t.Errorf("Leaking goroutines. %d alive!", leak)
	}
}

func TestThrottle(t *testing.T) {
	ch := make(chan int)
	workers := New(2)
	workers.Do("a", newResponder(ch, 1, 10*time.Millisecond).respond)
	workers.Do("a", newResponder(ch, 2, 15*time.Millisecond).respond)
	workers.Do("b", newResponder(ch, 3, 20*time.Millisecond).respond)
	for i := 1; i <= 3; i++ {
		v := <-ch
		if i != v {
			t.Errorf("Received %d, want %d\n", v, i)
		}
	}

	workers.Shutdown()
}

func TestShutdownAwait(t *testing.T) {
	time.Sleep(1) // Start the timer goroutine

	start := runtime.NumGoroutine()
	workers := New(0)
	workers.Do("a", func() {
		time.Sleep(50 * time.Millisecond)
	})

	workers.Shutdown()
	leak := runtime.NumGoroutine() - start
	if leak > 0 {
		t.Errorf("There are %d running task(s) after shutdown.", leak)
	}
}

type responder struct {
	ch    chan int
	value int
	delay time.Duration
}

func newResponder(ch chan int, value int, delay time.Duration) *responder {
	return &responder{ch, value, delay}
}

func (r *responder) respond() {
	time.Sleep(r.delay)
	r.ch <- r.value
}
