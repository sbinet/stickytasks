/*
 * Copyright (C) 2013 Chandra Sekar S
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stickytasks

import (
	"container/list"
)

type Scheduler struct {
	in       chan *stickyTask
	shutdown chan bool
	maxTasks int
}

type stickyTask struct {
	key  interface{}
	task func()
}

func New(maxTasks int) *Scheduler {
	s := &Scheduler{make(chan *stickyTask), make(chan bool), maxTasks}
	go s.start()
	return s
}

func (s *Scheduler) Do(key interface{}, task func()) {
	s.in <- &stickyTask{key, task}
}

func (s *Scheduler) Shutdown() {
	close(s.in)
	<-s.shutdown
}

func (s *Scheduler) start() {
	done := make(chan interface{})
	workers := newWorkers(s.maxTasks, done)

	for {
		select {
		case t, ok := <-s.in:
			if !ok {
				workers.awaitCompletion()
				s.shutdown <- true
				return
			}

			if workers.tooBusy() {
				workers.doNext(<-done)
			}

			workers.addTask(t)
		case key := <-done:
			workers.doNext(key)
		}
	}
}

type workers struct {
	channels          map[interface{}]chan func()
	queues            map[interface{}]*list.List
	done              chan interface{}
	maxTasks, running int
}

func newWorkers(maxTasks int, done chan interface{}) *workers {
	return &workers{
		make(map[interface{}]chan func()),
		make(map[interface{}]*list.List),
		done, maxTasks, 0,
	}
}

func (w *workers) addTask(t *stickyTask) {
	w.running++
	if queue, ok := w.queues[t.key]; !ok {
		ch := make(chan func())
		w.channels[t.key] = ch
		w.queues[t.key] = list.New()
		go w.spawn(t.key)
		ch <- t.task
	} else {
		queue.PushBack(t)
	}
}

func (w *workers) spawn(key interface{}) {
	ch := w.channels[key]
	for {
		if op, ok := <-ch; ok {
			op()
			w.done <- key
		} else {
			break
		}
	}
}

func (w *workers) doNext(key interface{}) {
	w.running--
	if ch, queue := w.channels[key], w.queues[key]; queue.Len() == 0 {
		close(ch)
		delete(w.channels, key)
		delete(w.queues, key)
	} else {
		t := queue.Remove(queue.Front()).(*stickyTask)
		ch <- t.task
	}
}

func (w *workers) tooBusy() bool {
	return w.maxTasks > 0 && w.running == w.maxTasks
}

func (w *workers) awaitCompletion() {
	for w.running != 0 {
		w.doNext(<-w.done)
	}
}
