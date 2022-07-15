package main

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/n-r-w/aworker"
)

func main() {
	const packetSize = 100
	var bufferSize = runtime.NumCPU() * 20000

	w := aworker.NewAWorker(bufferSize, packetSize, runtime.NumCPU(), processor, onError)
	w.Start()
	fmt.Println("started")

	for j := 0; j < bufferSize; j++ {
		w.SendMessage(strconv.Itoa(j))
	}

	// time.Sleep(time.Second * 10)

	fmt.Println("waiting done")
	for w.QueueSize() > 0 {
		time.Sleep(time.Millisecond)
	}

	fmt.Println("sending async")
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < runtime.NumCPU()*200; j++ {
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
	fmt.Println("processing: ", len(messages))
	for range messages {
		for i := 0; i < runtime.NumCPU()*5000; i++ {
			_ = float64(i) * float64(i) / 2.0
		}
	}
	return nil
}

func onError(err error) {
}
