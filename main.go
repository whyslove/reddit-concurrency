package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var errChannelNotFound = errors.New("channel not found")

const numChannels = 3
const numEvents = 3
const stopWorkEvent = "disconnect"

type Emitter struct {
	channels map[string]chan string //key -> channel
	mu       sync.Mutex             // use to access channels
}

func (e *Emitter) Attach() (key string, channel <-chan string) {
	ch := make(chan string, 0)
	key = uuid.New().String()

	e.mu.Lock()
	defer e.mu.Unlock()

	e.channels[key] = ch
	return key, ch
}

func (e *Emitter) SendEvent(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if event == stopWorkEvent {
		for _, ch := range e.channels {
			close(ch)
		}
		return
	}

	for _, ch := range e.channels {
		ch <- event
	}
}

func (e *Emitter) Disconnect(key string) (err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ch, ok := e.channels[key]
	if !ok {
		return errChannelNotFound
	}
	close(ch)
	delete(e.channels, key)
	return nil
}

func NewEmitter() *Emitter {
	return &Emitter{
		channels: make(map[string]chan string),
	}
}

func main() {
	emitter := NewEmitter()
	channels := []<-chan string{}
	keys := []string{}

	// Create channels
	for i := 0; i < numChannels; i++ {
		key, ch := emitter.Attach()
		channels = append(channels, ch)
		keys = append(keys, key)
	}

	// SendEvents
	go func() {
		for i := 0; i < numEvents; i++ {
			event := fmt.Sprintf("event %d", i)
			emitter.SendEvent(event)
		}
	}()

	signal := make(chan struct{})

	// Consume Events
	go func() {
		var wg sync.WaitGroup

		for j, channel := range channels {
			// Every consumer work in its own goroutine
			wg.Add(1)
			go func(j int, channel <-chan string) {
				defer wg.Done()

				for event := range channel {
					log.Info().Msgf("channel: %d, event: %s", j, event)
				}
			}(j, channel)

		}
		wg.Wait()
		signal <- struct{}{}
	}()

	// Emulate some work time
	time.Sleep(2 * time.Second)

	//Disconnect pseudo-random member
	emitter.Disconnect(keys[1])

	emitter.SendEvent("event after disconnect")
	emitter.SendEvent(stopWorkEvent)
	<-signal
	log.Info().Msg("shutdown")
}
