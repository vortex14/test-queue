package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type queue struct {
	mu      sync.Mutex // Локальная блокировка в рамках queue
	msgs    []string   // Общий набор сообщений
	waiters []*waiter  // Список консумеров
}

// pop забираем сообщение по FIFO
func (q *queue) pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.msgs) == 0 {
		return "", false
	}

	msg := q.msgs[0]
	q.msgs = q.msgs[1:]

	return msg, true
}

// addWaiter добавляем консумера который ждёт первый msg в queue
func (q *queue) addWaiter() *waiter {
	w := &waiter{ch: make(chan string, 1)}
	q.mu.Lock()
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()
	return w
}

// cancelWaiter - отменяем ожилание msg по timeout
func (q *queue) cancelWaiter(w *waiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	i := slices.Index(q.waiters, w)
	if i < 0 {
		return false
	}

	q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)

	return true
}

// put отправляем первому ожидающему консумеру или сохраняем msg
func (q *queue) put(msg string) {
	q.mu.Lock()
	if len(q.waiters) == 0 {
		q.msgs = append(q.msgs, msg)
		q.mu.Unlock()
		return
	}

	w := q.waiters[0]
	q.waiters = q.waiters[1:]
	q.mu.Unlock()

	w.ch <- msg
}

// get выдаёт сообщение из очереди первому по запросу либо подписывает на ожидание msg по timeout
func (q *queue) get(ctx context.Context, timeout time.Duration) (string, bool) {
	if msg, ok := q.pop(); ok {
		return msg, true
	}

	if timeout <= 0 {
		return "", false
	}

	w := q.addWaiter()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true

	case <-timer.C:
		if q.cancelWaiter(w) {
			return "", false
		}
		// waiter уже снят из очереди в put, ждём закреплённое сообщение
		msg := <-w.ch
		return msg, true

	case <-ctx.Done():
		if q.cancelWaiter(w) {
			return "", false
		}

		msg := <-w.ch
		return msg, true
	}
}

type broker struct {
	mu     sync.RWMutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	b.mu.RLock()
	q := b.queues[name]
	b.mu.RUnlock()

	if q != nil {
		return q
	}

	b.mu.Lock()
	if q = b.queues[name]; q != nil {
		b.mu.Unlock()
		return q
	}

	q = &queue{}
	b.queues[name] = q
	b.mu.Unlock()

	return q
}

func (b *broker) handlePut(w http.ResponseWriter, r *http.Request, q *queue) {
	v, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	q.put(v[0])
	w.WriteHeader(http.StatusOK)
}

func (b *broker) handleGet(w http.ResponseWriter, r *http.Request, q *queue) {
	timeout := time.Duration(0)
	if s := r.URL.Query().Get("timeout"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		timeout = time.Duration(n) * time.Second
	}

	msg, ok := q.get(r.Context(), timeout)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(msg)); err != nil {
		q.put(msg)
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(r.URL.Path, "/")
	if name == "" || strings.Contains(name, "/") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	q := b.getQueue(name)

	switch r.Method {
	case http.MethodPut:
		b.handlePut(w, r, q)
	case http.MethodGet:
		b.handleGet(w, r, q)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: app <port>")
		os.Exit(1)
	}
	addr := ":" + os.Args[1]
	if err := http.ListenAndServe(addr, newBroker()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
