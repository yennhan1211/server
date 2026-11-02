// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// parkHook blocks the first limit calls of the hook event `on` until release is closed,
// so a test can hold one goroutine at a chosen point while another one runs.
type parkHook struct {
	HookBase
	on      byte
	limit   int32
	calls   atomic.Int32
	arrived chan *Client
	release chan struct{}
	freed   sync.Once
}

func newParkHook(on byte, limit int) *parkHook {
	return &parkHook{
		on:      on,
		limit:   int32(limit),
		arrived: make(chan *Client, limit),
		release: make(chan struct{}),
	}
}

func (h *parkHook) ID() string                 { return "park-hook" }
func (h *parkHook) Provides(b byte) bool       { return b == h.on }
func (h *parkHook) OnClientExpired(cl *Client) { h.park(cl) }

func (h *parkHook) OnSessionEstablish(cl *Client, pk packets.Packet)   { h.park(cl) }
func (h *parkHook) OnSessionEstablished(cl *Client, pk packets.Packet) { h.park(cl) }
func (h *parkHook) OnDisconnect(cl *Client, err error, expire bool)    { h.park(cl) }
func (h *parkHook) OnUnsubscribed(cl *Client, pk packets.Packet)       { h.park(cl) }
func (h *parkHook) OnWill(cl *Client, will Will) (Will, error)         { h.park(cl); return will, nil }

// free releases parked and later calls; it is safe to call more than once. Tests defer
// it after Server.Close so a failing test does not leave Close waiting on a parked hook.
func (h *parkHook) free() { h.freed.Do(func() { close(h.release) }) }

func (h *parkHook) park(cl *Client) {
	if h.calls.Add(1) > h.limit {
		return
	}
	h.arrived <- cl
	<-h.release
}

func (h *parkHook) waitArrived(t *testing.T) *Client {
	t.Helper()
	select {
	case cl := <-h.arrived:
		return cl
	case <-time.After(2 * time.Second):
		require.FailNow(t, "hook was not reached")
		return nil
	}
}

// connectPacket returns a minimal MQTT v3.1.1 CONNECT for the client id.
func connectPacket(id string, clean bool) []byte {
	var flags byte
	if clean {
		flags = 1 << 1
	}

	b := []byte{packets.Connect << 4, byte(12 + len(id)), 0, 4, 'M', 'Q', 'T', 'T', 4, flags, 0, 60, 0, byte(len(id))}
	return append(b, id...)
}

// dialTestServer connects a client over a pipe and drains whatever the server sends.
// It returns the client side of the pipe.
func dialTestServer(s *Server, connect []byte) net.Conn {
	r, w := net.Pipe()
	go func() { _ = s.EstablishConnection("tcp", r) }()
	go func() { _, _ = w.Write(connect) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()
	return w
}

// registeredAndLive reports whether the client id is registered to a live client other than old.
func registeredAndLive(s *Server, id string, old *Client) bool {
	cl, ok := s.Clients.Get(id)
	return ok && cl != old && !cl.Closed()
}

// Connections with the same client id which reach the session takeover together must
// end with exactly one live session, and it must be the registered one. Previously each
// of them could take over the same old session and register itself, leaving the others
// connected but unregistered, so they silently stopped receiving messages.
func TestEstablishConnectionConcurrentSameClientID(t *testing.T) {
	const n = 16
	s := newServer()
	h := newParkHook(OnSessionEstablish, n)
	require.NoError(t, s.AddHook(h, nil))
	defer s.Close()
	defer h.free()

	conns := make([]net.Conn, n)
	for i := range conns {
		conns[i] = dialTestServer(s, connectPacket("zen", true))
	}

	clients := make([]*Client, n)
	for i := range clients {
		clients[i] = h.waitArrived(t)
	}
	h.free()

	live := func() (out []*Client) {
		for _, cl := range clients {
			if !cl.Closed() {
				out = append(out, cl)
			}
		}
		return out
	}

	require.Eventually(t, func() bool { return len(live()) == 1 }, 2*time.Second, 5*time.Millisecond)
	registered, ok := s.Clients.Get("zen")
	require.True(t, ok)
	require.Same(t, live()[0], registered)

	for _, c := range conns {
		_ = c.Close()
	}
	require.Eventually(t, func() bool {
		_, ok := s.Clients.Get("zen")
		return !ok
	}, 2*time.Second, 5*time.Millisecond)
}

// A clean session which ends on its own while a new connection with the same client id
// arrives must not delete the new session's entry during its cleanup.
func TestEstablishConnectionTeardownKeepsNewSession(t *testing.T) {
	s := newServer()
	h := newParkHook(OnUnsubscribed, 1)
	require.NoError(t, s.AddHook(h, nil))
	defer s.Close()
	defer h.free()

	oldConn := dialTestServer(s, connectPacket("zen", true))
	require.Eventually(t, func() bool { return registeredAndLive(s, "zen", nil) }, 2*time.Second, 5*time.Millisecond)
	old, _ := s.Clients.Get("zen")

	_ = oldConn.Close() // the old session's cleanup parks between its checks and its delete
	require.Same(t, old, h.waitArrived(t))

	newConn := dialTestServer(s, connectPacket("zen", true))
	defer newConn.Close()
	time.Sleep(50 * time.Millisecond) // let the new connection reach the takeover
	h.free()

	require.Eventually(t, func() bool { return registeredAndLive(s, "zen", old) }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.True(t, registeredAndLive(s, "zen", old))
}

// An expired session which reconnects while the expiry sweep runs must keep the new
// session's entry.
func TestClearExpiredClientsKeepsReconnectedSession(t *testing.T) {
	s := newServer()
	h := newParkHook(OnClientExpired, 1)
	require.NoError(t, s.AddHook(h, nil))
	defer s.Close()
	defer h.free()

	now := time.Now().Unix()
	expired, _, _ := newTestClient()
	expired.ID = "zen"
	expired.State.disconnected = now - 10
	expired.State.cancelOpen()
	expired.Properties.ProtocolVersion = 5
	expired.Properties.Props.SessionExpiryInterval = 8
	expired.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(expired)

	go s.clearExpiredClients(now)
	require.Same(t, expired, h.waitArrived(t))

	newConn := dialTestServer(s, connectPacket("zen", true))
	defer newConn.Close()
	time.Sleep(50 * time.Millisecond) // let the new connection reach the takeover
	h.free()

	require.Eventually(t, func() bool { return registeredAndLive(s, "zen", expired) }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.True(t, registeredAndLive(s, "zen", expired))
}

func TestClientLocksSerializeSameIDOnly(t *testing.T) {
	var l clientLocks

	unlockA := l.lock("a")
	unlockB := l.lock("b") // a different id is not blocked

	acquired := make(chan struct{})
	go func() {
		unlock := l.lock("a")
		close(acquired)
		unlock()
	}()

	select {
	case <-acquired:
		require.FailNow(t, "same id acquired while held")
	case <-time.After(50 * time.Millisecond):
	}

	unlockA()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "same id not acquired after release")
	}

	unlockB()
	l.mu.Lock()
	defer l.mu.Unlock()
	require.Empty(t, l.internal) // ids are dropped once unused
}

func TestClientLocksConcurrent(t *testing.T) {
	var l clientLocks
	var wg sync.WaitGroup
	ids := []string{"a", "b", "c", "d"}
	counts := make([]int, len(ids)) // counts[i] is guarded only by the lock of ids[i]

	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				unlock := l.lock(ids[k])
				counts[k]++
				unlock()
			}
		}(i % len(ids))
	}
	wg.Wait()

	for _, c := range counts {
		require.Equal(t, 16*100, c)
	}
	require.Empty(t, l.internal)
}

// A client id taken over after the expiry sweep took its snapshot must not be deleted
// by the sweep.
func TestClearExpiredClientsSkipsSessionTakenOverAfterSnapshot(t *testing.T) {
	s := newServer()
	h := newParkHook(OnClientExpired, 1)
	require.NoError(t, s.AddHook(h, nil))
	defer s.Close()
	defer h.free()

	now := time.Now().Unix()
	expired := map[string]*Client{}
	for _, id := range []string{"a", "b"} {
		cl, _, _ := newTestClient()
		cl.ID = id
		cl.State.disconnected = now - 10
		cl.State.cancelOpen()
		cl.Properties.ProtocolVersion = 5
		cl.Properties.Props.SessionExpiryInterval = 8
		cl.Properties.Props.SessionExpiryIntervalFlag = true
		s.Clients.Add(cl)
		expired[id] = cl
	}

	// The takeover below stops the old client again, which refreshes its disconnect time,
	// so sweep far enough ahead for it to still count as expired.
	go s.clearExpiredClients(now + 100)
	first := h.waitArrived(t) // the sweep holds its snapshot of both clients

	other := "a"
	if first.ID == "a" {
		other = "b"
	}
	newConn := dialTestServer(s, connectPacket(other, true))
	defer newConn.Close()
	require.Eventually(t, func() bool { return registeredAndLive(s, other, expired[other]) }, 2*time.Second, 5*time.Millisecond)
	h.free()

	time.Sleep(50 * time.Millisecond)
	require.True(t, registeredAndLive(s, other, expired[other]))
}

// A new session for a client id must not be established while OnDisconnect of the
// previous session (not taken over) is still running, or hooks which track presence
// see that disconnect after the new connection and mark a connected client offline.
func TestEstablishConnectionWaitsForPreviousOnDisconnect(t *testing.T) {
	s := newServer()
	disconnect := newParkHook(OnDisconnect, 1)
	established := newParkHook(OnSessionEstablished, 2)
	established.free() // record only
	require.NoError(t, s.AddHook(disconnect, nil))
	require.NoError(t, s.AddHook(established, nil))
	defer s.Close()
	defer disconnect.free()

	oldConn := dialTestServer(s, connectPacket("zen", true))
	old := established.waitArrived(t)

	_ = oldConn.Close()
	require.Same(t, old, disconnect.waitArrived(t))
	require.False(t, old.IsTakenOver())

	newConn := dialTestServer(s, connectPacket("zen", true))
	defer newConn.Close()
	time.Sleep(50 * time.Millisecond) // the new connection must be waiting, not established
	require.Empty(t, established.arrived)

	disconnect.free()
	require.NotSame(t, old, established.waitArrived(t))
	require.Eventually(t, func() bool { return registeredAndLive(s, "zen", old) }, 2*time.Second, 5*time.Millisecond)
}

// A session whose entry was removed by the expiry sweep while it was still shutting
// down, and whose id was then registered by a new connection, must leave the new
// session in place when its own cleanup finally runs.
func TestEstablishConnectionTeardownAfterExpiryKeepsNewSession(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumSessionExpiryInterval = 0
	h := newParkHook(OnWill, 1)
	require.NoError(t, s.AddHook(h, nil))
	defer s.Close()
	defer h.free()

	id := "zen"
	willConnect := []byte{packets.Connect << 4, byte(18 + len(id)), 0, 4, 'M', 'Q', 'T', 'T', 4, 1<<1 | 1<<2, 0, 60, 0, byte(len(id))}
	willConnect = append(willConnect, id...)
	willConnect = append(willConnect, 0, 1, 'w', 0, 1, 'x') // will topic and message

	oldConn := dialTestServer(s, willConnect)
	defer oldConn.Close()
	require.Eventually(t, func() bool { return registeredAndLive(s, id, nil) }, 2*time.Second, 5*time.Millisecond)
	old, _ := s.Clients.Get(id)

	_ = s.DisconnectClient(old, packets.ErrAdministrativeAction)
	require.Same(t, old, h.waitArrived(t)) // stopped, parked in sendLWT before its cleanup

	s.clearExpiredClients(time.Now().Unix() + 100)
	_, ok := s.Clients.Get(id)
	require.False(t, ok)

	newConn := dialTestServer(s, connectPacket(id, true))
	defer newConn.Close()
	require.Eventually(t, func() bool { return registeredAndLive(s, id, old) }, 2*time.Second, 5*time.Millisecond)
	h.free()

	time.Sleep(50 * time.Millisecond)
	require.True(t, registeredAndLive(s, id, old))
}
