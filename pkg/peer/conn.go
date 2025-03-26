package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/rtctunnel/rtctunnel/internal/signal"
	"github.com/rtctunnel/rtctunnel/pkg/crypt"
)

// Conn wraps an RTCPeerConnection so connections can be made and accepted.
type Conn struct {
	keypair       crypt.KeyPair
	peerPublicKey crypt.Key

	pc RTCPeerConnection

	closeCond *Cond
	closeErr  error

	incoming chan RTCDataChannel
}

// Accept accepts a new connection over the datachannel.
func (c *Conn) Accept() (net.Conn, int, error) {
	for chData := range c.incoming {
		lbl := chData.Label()

		idx := strings.LastIndexByte(lbl, ':')
		if idx < 0 {
			log.Info().Str("label", lbl).Msg("ignoring datachannel")

			continue
		}

		name := lbl[:idx]

		port, err := strconv.Atoi(lbl[idx+1:])
		if err != nil || name != "rtctunnel" {
			log.Info().Str("label", lbl).Msg("ignoring datachannel")

			continue
		}

		stream, err := WrapDataChannel(chData)
		if errors.Is(err, ErrClosedByPeer) {
			log.Info().Str("label", lbl).Msg("ignoring datachannel: closed by peer")

			continue
		} else if err != nil {
			return nil, 0, errors.Join(err, chData.Close())
		}

		log.Info().
			Str("peer", c.peerPublicKey.String()).
			Int("port", port).
			Msg("accepted connection")

		return stream, port, nil
	}

	return nil, 0, context.Canceled
}

// Open opens a new connection over the datachannel.
func (c *Conn) Open(port int) (net.Conn, error) {
	chData, err := c.pc.CreateDataChannel(fmt.Sprintf("rtctunnel:%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to open RTCDataChannel: %w", err)
	}

	stream, err := WrapDataChannel(chData)
	if err != nil {
		return nil, errors.Join(err, chData.Close())
	}

	log.Info().
		Str("peer", c.peerPublicKey.String()).
		Int("port", port).
		Msg("opened connection")

	return stream, err
}

// Close closes the peer connection.
func (c *Conn) Close() error {
	return c.closeWithError(c.closeErr)
}

func (c *Conn) closeWithError(err error) error {
	c.closeCond.Do(func() {
		if c.pc != nil {
			e := c.pc.Close()
			if err == nil {
				err = e
			}
		}
	})

	if c.closeErr != nil {
		err = c.closeErr
	}

	return err
}

// Open opens a new Connection.
func Open(
	ctx context.Context,
	keypair crypt.KeyPair,
	peerPublicKey crypt.Key,
	options ...signal.Option,
) (*Conn, error) {
	conn := &Conn{
		keypair:       keypair,
		peerPublicKey: peerPublicKey,
		closeCond:     NewCond(),
		incoming:      make(chan RTCDataChannel, 1),
	}

	log.Info().
		Str("peer", peerPublicKey.String()).
		Msg("creating webrtc peer connection")

	connected := NewCond()
	iceReady := NewCond()

	var (
		iceCandidates []string
		err           error
	)

	if conn.pc, err = NewRTCPeerConnection(); err != nil {
		return nil, fmt.Errorf("failed to create webrtc peer connection: %w", errors.Join(err, conn.Close()))
	}

	setCallbacks(conn, iceReady, iceCandidates, connected)

	if keypair.Public.String() < peerPublicKey.String() {
		if err := runInit(ctx, conn, iceReady, keypair, peerPublicKey, iceCandidates, options...); err != nil {
			return nil, conn.closeWithError(err)
		}
	} else {
		if err := runRecv(ctx, conn, iceReady, keypair, peerPublicKey, iceCandidates, options...); err != nil {
			return nil, conn.closeWithError(err)
		}
	}

	select {
	case <-time.After(time.Minute):
		return nil, conn.closeWithError(fmt.Errorf("failed to connect in time: %w", err))
	case <-connected.C:
	}

	return conn, nil
}

func setCallbacks(
	conn *Conn,
	iceReady *Cond,
	iceCandidates []string,
	connected *Cond,
) {
	conn.pc.OnICECandidate(func(candidate string) {
		if candidate == "" {
			iceReady.Signal()
		} else {
			iceCandidates = append(iceCandidates, candidate)
		}
	})

	conn.pc.OnICEConnectionStateChange(func(state string) {
		switch state {
		case "connected":
			connected.Signal()
		case "closed":
			_ = conn.closeWithError(context.Canceled)
		}
	})

	conn.pc.OnDataChannel(func(dc RTCDataChannel) {
		conn.incoming <- dc
	})
}

func runInit(
	ctx context.Context,
	conn *Conn,
	iceReady *Cond,
	keypair crypt.KeyPair,
	peerPublicKey crypt.Key,
	iceCandidates []string,
	options ...signal.Option,
) error {
	if _, err := conn.pc.CreateDataChannel("rtctunnel:init"); err != nil {
		return fmt.Errorf("error creating init datachannel: %w", err)
	}

	// we create the offer
	offer, err := conn.pc.CreateOffer()
	if err != nil {
		return fmt.Errorf("error creating webrtc offer: %w", err)
	}

	// wait for the ice candidates
	select {
	case <-iceReady.C:
	case <-conn.closeCond.C:
		return context.Canceled
	}

	if err = sendSignal(ctx, keypair, peerPublicKey, &SignalMessage{
		SDP:           offer,
		ICECandidates: iceCandidates,
	}, options...); err != nil {
		return fmt.Errorf("error sending offer: %w", err)
	}

	answer, err := recvSignal(ctx, keypair, peerPublicKey, options...)
	if err != nil {
		return fmt.Errorf("error receiving webrtc answer: %w", err)
	}

	if err = conn.pc.SetAnswer(answer.SDP); err != nil {
		return fmt.Errorf("error setting webrtc answer: %w", err)
	}

	for _, candidate := range answer.ICECandidates {
		if err = conn.pc.AddICECandidate(candidate); err != nil {
			return fmt.Errorf("error adding ice candidate: %w", err)
		}
	}

	return nil
}

func runRecv(
	ctx context.Context,
	conn *Conn,
	iceReady *Cond,
	keypair crypt.KeyPair,
	peerPublicKey crypt.Key,
	iceCandidates []string,
	options ...signal.Option,
) error {
	offer, err := recvSignal(ctx, keypair, peerPublicKey, options...)
	if err != nil {
		return fmt.Errorf("error receiving webrtc offer: %w", err)
	}

	if err = conn.pc.SetOffer(offer.SDP); err != nil {
		return fmt.Errorf("error setting webrtc offer: %w", err)
	}

	answer, err := conn.pc.CreateAnswer()
	if err != nil {
		return fmt.Errorf("error creating webrtc answer: %w", err)
	}

	for _, candidate := range offer.ICECandidates {
		err = conn.pc.AddICECandidate(candidate)
		if err != nil {
			return fmt.Errorf("error adding ice candidate: %w", err)
		}
	}

	// wait for the ice candidates
	select {
	case <-iceReady.C:
	case <-conn.closeCond.C:
		return context.Canceled
	}

	if err = sendSignal(ctx, keypair, peerPublicKey, &SignalMessage{
		SDP:           answer,
		ICECandidates: iceCandidates,
	}, options...); err != nil {
		return fmt.Errorf("error marshaling signal message: %w", err)
	}

	return nil
}

type SignalMessage struct {
	SDP           string
	ICECandidates []string
}

func recvSignal(
	ctx context.Context,
	keypair crypt.KeyPair,
	peerPublicKey crypt.Key,
	options ...signal.Option,
) (*SignalMessage, error) {
	bs, err := signal.Recv(ctx, keypair, peerPublicKey, options...)
	if err != nil {
		return nil, err
	}

	var msg SignalMessage
	if err = json.Unmarshal(bs, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

func sendSignal(
	ctx context.Context,
	keypair crypt.KeyPair,
	peerPublicKey crypt.Key,
	msg *SignalMessage,
	options ...signal.Option,
) error {
	bs, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if err = signal.Send(ctx, keypair, peerPublicKey, bs, options...); err != nil {
		return err
	}

	return nil
}
