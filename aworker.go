// Package aworker асинхронная фоновая обработка произвольных сообщений
package aworker

import (
	"sync"
	"sync/atomic"
	"time"
)

type ProcessorFunc func(messages []any) error
type ErrorFunc func(err error)

type AWorker struct {
	mu               sync.Mutex
	started          bool
	buffer           chan any
	workerCount      int
	currentQueueSize int32
	packetSize       int

	chanOnExit chan struct{}
	wgExit     sync.WaitGroup

	processorFunc ProcessorFunc
	errorFunc     ErrorFunc
}

func NewAWorker(queueSize int, packetSize int, workerCount int, processorFunc ProcessorFunc, errorFunc ErrorFunc) *AWorker {
	s := &AWorker{
		mu:               sync.Mutex{},
		started:          false,
		buffer:           make(chan any, queueSize),
		workerCount:      workerCount,
		currentQueueSize: 0,
		packetSize:       packetSize,
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
			s.processMessages(true)

			defer s.wgExit.Done()
			return

		default:
			s.processMessages(false)
			// вынужденный компромисс, т.к. в противном случае придется передавать сообщение на обработку
			// по одному, по мере получения их из канала, вместо того, чтобы группировать
			time.Sleep(time.Millisecond)
		}
	}
}

func (s *AWorker) processMessages(all bool) {
	var messages []any
	hasData := true
	var m any
	for hasData {
		select {
		case m, hasData = <-s.buffer:
			if hasData {
				messages = append(messages, m)

				if len(messages) >= s.packetSize {
					s.processMessagesHelper(messages)
					messages = nil
					if !all {
						hasData = false
					}
				}
			}
		default:
			hasData = false
		}
	}

	s.processMessagesHelper(messages)
}

func (s *AWorker) processMessagesHelper(messages []any) {
	if len(messages) == 0 {
		return
	}

	err := s.processorFunc(messages)
	atomic.AddInt32(&s.currentQueueSize, int32(-len(messages)))
	if err != nil && s.errorFunc != nil {
		s.errorFunc(err)
	}
}
