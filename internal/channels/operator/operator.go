package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/rtctunnel/rtctunnel/internal/channels"
)

func init() {
	channels.RegisterFactory("operator", func(addr string) (channels.Channel, error) {
		return New(strings.Replace(addr, "operator://", "https://", 1)), nil
	})
}

var _ channels.Channel = (*Channel)(nil)

const DefaultClientTimeout = 30 * time.Second

// An operator.Channel signals over a custom http server.
type Channel struct {
	url    string
	client *http.Client
}

// New creates a new operator.Channel.
func New(url string) *Channel {
	return &Channel{
		url: url,
		client: &http.Client{
			Timeout: DefaultClientTimeout,
		},
	}
}

// Recv receives a message at the given key.
func (c *Channel) Recv(ctx context.Context, key string) (string, error) {
	log.Debug().Str("url", c.url).Str("key", key).Msg("[operator] receive")

	values := url.Values{
		"address": {key},
	}

	path := c.url + "/sub?" + values.Encode()

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		if err != nil {
			return "", err
		}

		resp, err := c.do(req)
		if err != nil {
			var nerr net.Error
			if errors.As(err, &nerr) && nerr.Timeout() {
				log.Warn().Msg("[operator] timed-out, retrying")

				continue
			}

			return "", err
		}

		if resp.StatusCode == http.StatusGatewayTimeout {
			log.Warn().Msg("[operator] timed-out, retrying")
			resp.Body.Close()

			continue
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return "", errors.New(resp.Status)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		log.Debug().Str("key", key).Str("data", string(data)).Msg("[operator] received")

		return string(data), nil
	}
}

// Send sends a message to the given key with the given data.
func (c *Channel) Send(ctx context.Context, key, data string) error {
	log.Debug().Str("url", c.url).Str("key", key).Str("data", data).Msg("[operator] send")

	values := url.Values{
		"address": {key},
		"data":    {data},
	}

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/pub", strings.NewReader(values.Encode()))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := c.do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusGatewayTimeout {
			if err := resp.Body.Close(); err != nil {
				log.Error().Msg(fmt.Sprintf("response body failed to close: %s", err))
			}

			continue
		}

		log.Debug().Int("status_code", resp.StatusCode).Str("status", resp.Status).Msg("[operator] sent")

		if _, err := io.ReadAll(resp.Body); err != nil {
			if err := resp.Body.Close(); err != nil {
				log.Error().Msg(fmt.Sprintf("response body failed to close: %s", err))
			}

			return err
		}

		if err := resp.Body.Close(); err != nil {
			log.Error().Msg(fmt.Sprintf("response body failed to close: %s", err))
		}

		return nil
	}
}

func (c *Channel) do(req *http.Request) (*http.Response, error) {
	if runtime.GOOS == "js" {
		req.Header.Set("js.fetch:mode", "cors")
	}

	for {
		res, err := c.client.Do(req)
		if err != nil && strings.Contains(c.url, "https://") && strings.Contains(err.Error(), "server gave HTTP response") {
			c.url = strings.Replace(c.url, "https://", "http://", 1)

			continue
		}

		return res, err
	}
}
