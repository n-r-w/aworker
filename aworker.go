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
	wakeup           chan struct{}
	workerCount      int
	currentQueueSize int32
	packetSize       int
	wgExit           sync.WaitGroup

	processorFunc ProcessorFunc
	errorFunc     ErrorFunc
}

/* NewAWorker создание обработчика
queueSize - размер буфера сообщений
packetSize - по сколько сообщений за раз максимум будет передаваться в processorFunc
workerCount - количество горутин на параллельную обработку
processorFunc = функция обработки сообщений
errorFunc - функция, в которую будут отправляться ошибки (может быть nil) */
func NewAWorker(queueSize int, packetSize int, workerCount int, processorFunc ProcessorFunc, errorFunc ErrorFunc) *AWorker {
	s := &AWorker{
		mu:               sync.Mutex{},
		started:          false,
		buffer:           make(chan any, queueSize),
		wakeup:           make(chan struct{}),
		workerCount:      workerCount,
		currentQueueSize: 0,
		packetSize:       packetSize,
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

	close(s.wakeup)
	close(s.buffer)
	s.wgExit.Wait() // ждем пока будет выход из воркеров
}

func (s *AWorker) QueueSize() int {
	return int(atomic.LoadInt32(&s.currentQueueSize))
}

func (s *AWorker) SendMessage(message any) {
	s.mu.Lock()
	if !s.started {
		panic("sent to stopped service")
	}
	s.mu.Unlock()

	s.buffer <- message
	s.wakeup <- struct{}{}

	atomic.AddInt32(&s.currentQueueSize, 1)
}

func (s *AWorker) worker() {
	defer s.wgExit.Done()

	for {
		var messages []any
		for {
			select {
			case m, hasData := <-s.buffer:
				if hasData {
					messages = append(messages, m)

					if len(messages) >= s.packetSize {
						s.processMessages(messages)
						messages = nil
					}
				} else {
					s.processMessages(messages)
					return
				}
			default:
				s.processMessages(messages)
				messages = nil

				// переходим в режим ожидания
				<-s.wakeup
			}
		}
	}
}

func (s *AWorker) processMessages(messages []any) {
	if len(messages) == 0 {
		return
	}

	err := s.processorFunc(messages)
	atomic.AddInt32(&s.currentQueueSize, int32(-len(messages)))
	if err != nil && s.errorFunc != nil {
		s.errorFunc(err)
	}
}
