package main

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/n-r-w/aworker"
)

var w *aworker.AWorker

func main() {
	const packetSize = 100
	var bufferSize = 1000

	w = aworker.NewAWorker(bufferSize, packetSize, runtime.NumCPU(), processor, onError)
	w.Start()
	fmt.Println("started")

	for j := 0; j < bufferSize; j++ {
		w.SendMessage(strconv.Itoa(j))
	}

	// time.Sleep(time.Second * 5)

	fmt.Println("waiting done")
	for w.QueueSize() > 0 {
		time.Sleep(time.Millisecond)
	}

	fmt.Println("sending async")
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				w.SendMessage(strconv.Itoa(j))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	w.Stop()

	if w.QueueSize() != 0 {
		panic("internal error")
	}
}

func processor(messages []any) error {
	fmt.Printf("processing count: %d, queue size: %d\n", len(messages), w.QueueSize())
	for range messages {
		for i := 0; i < runtime.NumCPU()*10000; i++ {
			_ = float64(i) * float64(i) / 2.0
		}
	}
	return nil
}

func onError(err error) {
}
