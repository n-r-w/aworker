// Package aworker асинхронная фоновая обработка произвольных сообщений
package aworker

import (
	"sync"
	"sync/atomic"
)

type ProcessorFunc func(message any) error
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
		go s.worker()
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

func (s *AWorker) worker() {
	for {
		select {
		case <-s.chanOnExit:
			for {
				if m, ok := <-s.buffer; ok {
					err := s.processorFunc(m)
					atomic.AddInt32(&s.currentQueueSize, -1)
					if err != nil && s.errorFunc != nil {
						s.errorFunc(err)
					}
				} else {
					defer s.wgExit.Done()
					return
				}
			}

		case m, ok := <-s.buffer:
			if ok {
				err := s.processorFunc(m)
				atomic.AddInt32(&s.currentQueueSize, -1)
				if err != nil && s.errorFunc != nil {
					s.errorFunc(err)
				}
			}
		}
	}
}
