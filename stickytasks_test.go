package stickytasks

import (
	"runtime"
	"testing"
	"time"
)

func TestStickiness(t *testing.T) {
	ch := make(chan int)
	start := runtime.NumGoroutine()
	workers := New(-1)
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
	leak := runtime.NumGoroutine() - start - 1 // Timer
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
