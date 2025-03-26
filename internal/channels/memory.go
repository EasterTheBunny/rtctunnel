package channels

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
)

func init() {
	RegisterFactory("memory", func(addr string) (Channel, error) {
		addr = addr[len("memory://"):]
		ch, err := newMemoryChannel(addr)

		return ch, err
	})
}

//nolint:gochecknoglobals
var memoryChannels = struct {
	sync.RWMutex
	channels map[string]chan string
}{
	channels: map[string]chan string{},
}

type memoryChannel struct {
	prefix string
}

// newMemoryChannel creates a new memoryChannel
func newMemoryChannel(addr string) (*memoryChannel, error) {
	return &memoryChannel{prefix: addr}, nil
}

func (c *memoryChannel) Send(ctx context.Context, key, data string) error {
	log.Debug().Str("key", key).Str("data", data).Msg("[MemoryChannel] sending")

	select {
	case c.getChannel(key) <- data:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (c *memoryChannel) Recv(ctx context.Context, key string) (string, error) {
	log.Debug().Str("key", key).Msg("[MemoryChannel] receiving")

	var data string

	select {
	case data = <-c.getChannel(key):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	return data, nil
}

func (c *memoryChannel) getChannel(key string) chan string {
	key = c.prefix + key

	memoryChannels.RLock()
	chMem, exists := memoryChannels.channels[key]
	memoryChannels.RUnlock()

	if !exists {
		memoryChannels.Lock()

		if chMem, exists = memoryChannels.channels[key]; !exists {
			chMem = make(chan string, 1)
			memoryChannels.channels[key] = chMem
		}

		memoryChannels.Unlock()
	}

	return chMem
}
