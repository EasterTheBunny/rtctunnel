package channels

import (
	"context"
	"fmt"
	"net/url"
	"sync"
)

// A Channel facilitates signaling.
type Channel interface {
	Send(ctx context.Context, key, data string) error
	Recv(ctx context.Context, key string) (data string, err error)
}

// A Factory returns a Channel from an address.
type Factory = func(addr string) (Channel, error)

//nolint:gochecknoglobals
var channelFactories = struct {
	sync.Mutex
	m map[string]Factory
}{
	m: make(map[string]Factory),
}

// RegisterFactory registers a new Factory.
func RegisterFactory(scheme string, factory Factory) {
	channelFactories.Lock()
	channelFactories.m[scheme] = factory
	channelFactories.Unlock()
}

// Get returns a channel for the given address.
//
//nolint:ireturn
func Get(strAddr string) (Channel, error) {
	addr, err := url.Parse(strAddr)
	if err != nil {
		return nil, err
	}

	channelFactories.Lock()
	factory, exists := channelFactories.m[addr.Scheme]
	channelFactories.Unlock()

	if !exists {
		return nil, fmt.Errorf("no channel factory registered for %s", addr.Scheme)
	}

	return factory(strAddr)
}

// Must panics if there's an error.
//
//nolint:ireturn
func Must(ch Channel, err error) Channel {
	if err != nil {
		panic(err)
	}

	return ch
}
