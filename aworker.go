// Package aworker асинхронная фоновая обработка произвольных сообщений
package aworker

import (
	"sync"
	"sync/atomic"
)

type ProcessorFunc func(messages []any) error
type ErrorFunc func(err error)

type AWorker struct {
	mu               sync.Mutex
	started          bool
	buffer           chan any
	workerCount      int
	currentQueueSize int32

	chanOnExit chan struct{}
	wgExit     sync.WaitGroup

	processorFunc ProcessorFunc
	errorFunc     ErrorFunc
}

func NewAWorker(queueSize int, workerCount int, processorFunc ProcessorFunc, errorFunc ErrorFunc) *AWorker {
	s := &AWorker{
		mu:               sync.Mutex{},
		started:          false,
		buffer:           make(chan any, queueSize),
		workerCount:      workerCount,
		currentQueueSize: 0,
		chanOnExit:       make(chan struct{}),
		wgExit:           sync.WaitGroup{},
		processorFunc:    processorFunc,
		errorFunc:        errorFunc,
	}

	return s
}

func (s *AWorker) Start() {
	s.mu.Lock()
	if s.started {
		panic("already started")
	}
	s.started = true
	for i := 0; i < s.workerCount; i++ {
		s.wgExit.Add(1)
		num := i
		go s.worker(num)
	}
	s.mu.Unlock()
}

func (s *AWorker) Stop() {
	s.mu.Lock()
	s.started = false
	s.mu.Unlock()

	close(s.buffer)
	for i := 0; i < s.workerCount; i++ {
		s.chanOnExit <- struct{}{}
	}
	s.wgExit.Wait() // ждем пока будет выход из воркеров
	close(s.chanOnExit)
}

func (s *AWorker) QueueSize() int {
	return int(atomic.LoadInt32(&s.currentQueueSize))
}

func (s *AWorker) SendMessage(message any) {
	s.mu.Lock()

	if !s.started {
		panic("sent to stopped service")
	}

	s.buffer <- message
	s.mu.Unlock()

	atomic.AddInt32(&s.currentQueueSize, 1)
}

func (s *AWorker) worker(number int) {
	for {
		select {
		case <-s.chanOnExit:
			s.processMessages(number)

			defer s.wgExit.Done()
			return

		default:
			s.processMessages(number)
		}
	}
}

func (s *AWorker) processMessages(number int) {
	var messages []any
	for m := range s.buffer {
		messages = append(messages, m)
	}
	if len(messages) == 0 {
		return
	}
	// fmt.Printf("worker %d, messages %d\n", number+1, len(messages))
	err := s.processorFunc(messages)
	atomic.AddInt32(&s.currentQueueSize, int32(-len(messages)))
	if err != nil && s.errorFunc != nil {
		s.errorFunc(err)
	}
}
