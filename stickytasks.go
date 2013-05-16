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

type Workers struct {
	in        chan stickyTask
	maxQueued int
}

func New(maxQueued int) *Workers {
	workers := &Workers{make(chan stickyTask), maxQueued}

	go workers.start()
	return workers
}

func (w *Workers) Do(key interface{}, task func()) {
	w.in <- stickyTask{key, task}
}

func (w *Workers) Shutdown() {
	close(w.in)
}

func (w *Workers) start() {
	done := make(chan interface{})
	channels := make(map[interface{}]chan func())
	queues := make(map[interface{}]*list.List)

	for {
		select {
		case t, ok := <-w.in:
			if !ok {
				break
			}

			if queue, ok := queues[t.key]; !ok {
				channels[t.key], queues[t.key] = spawnAndDo(t.key, t.task, done)
			} else {
				queue.PushBack(t)
			}

		case key := <-done:
			if ch, queue := channels[key], queues[key]; queue.Len() == 0 {
				close(ch)
				delete(channels, key)
				delete(queues, key)
			} else {
				t := queue.Remove(queue.Front()).(stickyTask)
				ch <- t.task
			}
		}
	}
}

type stickyTask struct {
	key  interface{}
	task func()
}

func spawnAndDo(key interface{}, task func(), done chan interface{}) (ch chan func(), queue *list.List) {
	ch = make(chan func())
	queue = list.New()
	go worker(key, ch, done)
	ch <- task
	return
}

func worker(key interface{}, ch chan func(), done chan interface{}) {
	for {
		select {
		case op, ok := <-ch:
			if !ok {
				break
			}

			op()
		default:
			done <- key
		}
	}
}