/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sshproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/ssh"

	"frontend/pkg/common/faas_common/constant"
	"frontend/pkg/common/faas_common/logger/log"
	"frontend/pkg/common/faas_common/types"
	"frontend/pkg/frontend/proxyrouting"
)

type server struct {
	config   *serverConfig
	listener net.Listener
	slots    chan struct{}
}

const (
	hostUserCreateOption  = "host_user"
	backendHandshakeWait  = 10 * time.Second
	channelCopyDirections = 2
)

type backendConnection struct {
	connection ssh.Conn
	channels   <-chan ssh.NewChannel
	requests   <-chan *ssh.Request
	tunnel     net.Conn
}

// Start starts the SSH bastion when YR_FRONTEND_SSH_ENABLE is true.
func Start(stopCh <-chan struct{}) error {
	config, enabled, err := loadServerConfig()
	if err != nil || !enabled {
		return err
	}
	listener, err := net.Listen("tcp", config.address)
	if err != nil {
		return fmt.Errorf("listen on frontend SSH address %s: %w", config.address, err)
	}
	srv := &server{config: config, listener: listener, slots: make(chan struct{}, config.connectionLimit)}
	go func() {
		<-stopCh
		if closeErr := listener.Close(); closeErr != nil && !isClosedNetworkError(closeErr) {
			log.GetLogger().Warnf("close frontend SSH listener failed: %s", closeErr)
		}
	}()
	go srv.serve()
	log.GetLogger().Infof("FaaS-Frontend SSH server started on %s, client public key auth: %t",
		config.address, config.clientAuth)
	if config.tunnelTLSConfig == nil {
		log.GetLogger().Warn("FaaS-Frontend SSH tunnel mTLS is disabled by the global component TLS setting")
	}
	return nil
}

func (s *server) serve() {
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			if isClosedNetworkError(err) {
				return
			}
			log.GetLogger().Warnf("accept frontend SSH connection failed: %s", err)
			continue
		}
		if s.acquireConnection() {
			go func() {
				defer s.releaseConnection()
				s.handleConnection(connection)
			}()
		} else {
			log.GetLogger().Warnf("reject frontend SSH connection from %s: concurrent connection limit reached",
				connection.RemoteAddr())
			closeWithLog("frontend SSH connection", connection)
		}
	}
}

func (s *server) acquireConnection() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *server) releaseConnection() {
	<-s.slots
}

func (s *server) handleConnection(connection net.Conn) {
	defer closeWithLog("frontend SSH connection", connection)
	if err := connection.SetDeadline(time.Now().Add(sshHandshakeTimeout)); err != nil {
		log.GetLogger().Warnf("set frontend SSH handshake deadline failed for %s: %s", connection.RemoteAddr(), err)
		return
	}
	clientConn, clientChannels, clientRequests, err := ssh.NewServerConn(connection, s.config.serverSSH)
	if err != nil {
		log.GetLogger().Warnf("frontend SSH handshake failed from %s: %s", connection.RemoteAddr(), err)
		return
	}
	if err = connection.SetDeadline(time.Time{}); err != nil {
		log.GetLogger().Warnf("clear frontend SSH handshake deadline failed for %s: %s", connection.RemoteAddr(), err)
		return
	}
	defer closeWithLog("frontend SSH client", clientConn)
	targetRoute, err := parseRoute(clientConn.User())
	if err != nil {
		return
	}
	// RFC 0016: trace id from the optional trace= username option; SSH has no
	// HTTP header, so fall back to a minted uuid (same fallback as HTTP paths).
	traceID := targetRoute.TraceID
	if traceID == "" {
		traceID = uuid.NewString()
	}
	start := time.Now()
	log.GetLogger().Info(fmt.Sprintf("[agent.ssh.enter] sshproxy trace_id=%s instance_id=%s",
		traceID, targetRoute.InstanceID))
	defer func() {
		log.GetLogger().Info(fmt.Sprintf("[agent.ssh.exit] sshproxy trace_id=%s instance_id=%s cost_ms=%d",
			traceID, targetRoute.InstanceID, time.Since(start).Milliseconds()))
	}()
	instance, tunnelAddress, err := s.resolveInstance(targetRoute)
	if err != nil {
		log.GetLogger().Warnf("resolve frontend SSH target failed trace_id=%s: %s", traceID, err)
		return
	}
	subject := ""
	if clientConn.Permissions != nil {
		subject = clientConn.Permissions.Extensions["subject"]
	}
	if err = s.config.authorizer.authorizeResolvedTarget(subject, targetRoute, instance); err != nil {
		log.GetLogger().Warnf("authorize frontend SSH target failed trace_id=%s: %s", traceID, err)
		return
	}
	requestID := uuid.NewString()
	backend, err := s.dialBackend(
		instance, tunnelAddress, targetRoute.TargetPort, requestID, traceID)
	if err != nil {
		log.GetLogger().Warnf("backend SSH handshake for instance %s trace_id=%s failed: %s",
			instance.InstanceID, traceID, err)
		return
	}
	defer closeWithLog("frontend SSH tunnel", backend.tunnel)
	defer closeWithLog("frontend SSH backend", backend.connection)
	go proxyGlobalRequests(clientRequests, backend.connection)
	go proxyGlobalRequests(backend.requests, clientConn)
	go proxyChannels(clientChannels, backend.connection)
	go proxyChannels(backend.channels, clientConn)

	done := make(chan struct{}, 2)
	go func() { _ = clientConn.Wait(); done <- struct{}{} }()
	go func() { _ = backend.connection.Wait(); done <- struct{}{} }()
	<-done
}

func (s *server) dialBackend(instance *types.InstanceSpecification, tunnelAddress string,
	targetPort int, requestID string, traceID string,
) (*backendConnection, error) {
	backendConfig := &ssh.ClientConfig{
		User:            instance.CreateOptions[hostUserCreateOption],
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(s.config.backendSigner)},
		HostKeyCallback: s.config.backendHostKey,
		Timeout:         backendHandshakeWait,
	}
	var lastErr error
	for attempt := 1; attempt <= s.config.backendAttempts; attempt++ {
		tunnel, dialErr := dialTunnel(tunnelAddress, s.config.tunnelTLSConfig, tunnelHeader{
			TunnelVersion: tunnelVersion,
			InstanceID:    instance.InstanceID,
			Protocol:      "tcp",
			TargetPort:    targetPort,
			RequestID:     requestID,
			TraceID:       traceID,
		})
		if dialErr == nil {
			_ = tunnel.SetDeadline(time.Now().Add(backendHandshakeWait))
			backendConn, channels, requests, handshakeErr := ssh.NewClientConn(
				tunnel, net.JoinHostPort(instance.InstanceID, "22"), backendConfig)
			if handshakeErr == nil {
				_ = tunnel.SetDeadline(time.Time{})
				return &backendConnection{
					connection: backendConn,
					channels:   channels,
					requests:   requests,
					tunnel:     tunnel,
				}, nil
			}
			lastErr = handshakeErr
			_ = tunnel.Close()
		} else {
			lastErr = dialErr
		}
		if attempt < s.config.backendAttempts {
			time.Sleep(s.config.backendInterval)
		}
	}
	return nil, fmt.Errorf("backend not ready after %d attempts: %w", s.config.backendAttempts, lastErr)
}

func (s *server) resolveInstance(target route) (*types.InstanceSpecification, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.routeWait)
	defer cancel()
	ownerRoute, err := proxyrouting.Wait(
		ctx, target.InstanceID, proxyrouting.CapabilityTCPTunnel, proxyrouting.TransportTCPTunnel)
	if err != nil {
		return nil, "", err
	}
	instance := ownerRoute.Instance
	if instance.InstanceStatus.Code != int32(constant.KernelInstanceStatusRunning) {
		return nil, "", fmt.Errorf("instance %s is not running", target.InstanceID)
	}
	if strings.TrimSpace(instance.CreateOptions[hostUserCreateOption]) == "" {
		return nil, "", fmt.Errorf("instance %s has no host_user create option", target.InstanceID)
	}
	return instance, ownerRoute.Address, nil
}

type requestSender interface {
	SendRequest(name string, wantReply bool, payload []byte) (bool, []byte, error)
}

func proxyGlobalRequests(requests <-chan *ssh.Request, destination requestSender) {
	for request := range requests {
		accepted, response, err := destination.SendRequest(request.Type, request.WantReply, request.Payload)
		if err != nil {
			log.GetLogger().Warnf("proxy SSH global request %s failed: %s", request.Type, err)
		}
		if request.WantReply {
			_ = request.Reply(err == nil && accepted, response)
		}
	}
}

func proxyChannels(channels <-chan ssh.NewChannel, destination ssh.Conn) {
	for newChannel := range channels {
		go proxyChannel(newChannel, destination)
	}
}

func proxyChannel(newChannel ssh.NewChannel, destination ssh.Conn) {
	destinationChannel, destinationRequests, err := destination.OpenChannel(
		newChannel.ChannelType(), newChannel.ExtraData())
	if err != nil {
		_ = newChannel.Reject(ssh.ConnectionFailed, err.Error())
		return
	}
	sourceChannel, sourceRequests, err := newChannel.Accept()
	if err != nil {
		destinationChannel.Close()
		return
	}
	defer sourceChannel.Close()
	defer destinationChannel.Close()
	go proxyChannelRequests(sourceRequests, destinationChannel)
	destinationRequestsDone := make(chan struct{})
	go func() {
		proxyChannelRequests(destinationRequests, sourceChannel)
		close(destinationRequestsDone)
	}()
	var wait sync.WaitGroup
	wait.Add(channelCopyDirections)
	go copyChannel(destinationChannel, sourceChannel, &wait)
	go copyChannel(sourceChannel, destinationChannel, &wait)
	wait.Wait()
	// The backend can send exit-status after EOF. Drain its channel requests
	// before closing the client channel so OpenSSH receives the command status.
	<-destinationRequestsDone
}

func proxyChannelRequests(requests <-chan *ssh.Request, destination ssh.Channel) {
	for request := range requests {
		accepted, err := destination.SendRequest(request.Type, request.WantReply, request.Payload)
		if request.WantReply {
			_ = request.Reply(err == nil && accepted, nil)
		}
	}
}

func copyChannel(destination, source ssh.Channel, wait *sync.WaitGroup) {
	defer wait.Done()
	if _, err := io.Copy(destination, source); err != nil && !isClosedNetworkError(err) {
		log.GetLogger().Warnf("copy SSH channel failed: %s", err)
	}
	if err := destination.CloseWrite(); err != nil && !isClosedNetworkError(err) {
		log.GetLogger().Warnf("close SSH channel writer failed: %s", err)
	}
}

func closeWithLog(name string, closer io.Closer) {
	if err := closer.Close(); err != nil && !isClosedNetworkError(err) {
		log.GetLogger().Warnf("close %s failed: %s", name, err)
	}
}

func isClosedNetworkError(err error) bool {
	return err == net.ErrClosed || (err != nil && err.Error() == "use of closed network connection")
}
