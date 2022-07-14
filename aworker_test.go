// Package aworker асинхронная фоновая обработка произвольных сообщений
package aworker

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
)

func Test(t *testing.T) {
	w := NewAWorker(100, runtime.NumCPU(), processor, onError)
	w.Start()

	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < runtime.NumCPU()*2000; j++ {
				w.SendMessage(strconv.Itoa(j))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	w.Stop()

	if w.currentQueueSize != 0 {
		t.Error("internal error")
	}
}

func processor(message any) error {
	for i := 0; i < runtime.NumCPU()*4000; i++ {
		_ = float64(i) * float64(i) / 2.0
	}
	return nil
}

func onError(err error) {
}
