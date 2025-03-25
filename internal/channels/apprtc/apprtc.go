package apprtc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/websocket"

	"github.com/rtctunnel/rtctunnel/internal/channels"
)

func init() {
	channels.RegisterFactory("apprtc", func(_ string) (channels.Channel, error) {
		return New(), nil
	})
}

var _ channels.Channel = (*Channel)(nil)

const DefaultAppRTCURL = "wss://apprtc-ws.webrtc.org/ws"

// An apprtc.Channel signals over apprtc.
type Channel struct{}

// New creates a new apprtc.Channel.
func New() *Channel {
	return &Channel{}
}

// Recv receives a message at the given key.
func (c *Channel) Recv(ctx context.Context, key string) (string, error) {
	conn, err := c.getConnection(ctx, key, "recv")
	if err != nil {
		return "", err
	}

	defer conn.Close()

	var packet struct {
		Message string `json:"msg"`
		Error   string `json:"error"`
	}

	if err = conn.ReadJSON(&packet); err != nil {
		return "", fmt.Errorf("error receiving packet: %w", err)
	}

	if packet.Error != "" {
		return "", fmt.Errorf("apprtc returned an error: %s", packet.Error)
	}

	return packet.Message, nil
}

// Send sends a message to the given key with the given data.
func (c *Channel) Send(ctx context.Context, key, data string) error {
	conn, err := c.getConnection(ctx, key, "send")
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.WriteJSON(map[string]interface{}{
		"cmd": "send",
		"msg": data,
	})
	if err != nil {
		return fmt.Errorf("error sending over websocket: %w", err)
	}

	return nil
}

func (c *Channel) getConnection(ctx context.Context, roomID, clientID string) (*websocket.Conn, error) {
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, DefaultAppRTCURL, http.Header{
		"Origin": {"https://appr.tc"},
	})
	if err != nil {
		var msg string

		if resp.Body != nil {
			bs, rErr := io.ReadAll(resp.Body)
			msg = string(bs)

			err = errors.Join(err, rErr, resp.Body.Close())
		}

		return nil, fmt.Errorf("error connecting to webrtc (msg=%s): %w", msg, err)
	}

	if err := conn.WriteJSON(map[string]any{
		"cmd":      "register",
		"roomid":   roomID,
		"clientid": clientID,
	}); err != nil {
		err = errors.Join(err, conn.Close())

		return nil, fmt.Errorf("error registering send client: %w", err)
	}

	return conn, nil
}
