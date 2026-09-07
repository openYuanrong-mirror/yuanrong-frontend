/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

package route

import (
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
)

// portForwardKey mirrors the C++ PORT_FORWARD_KEY extension key.
const portForwardKey = "portForward"

const (
	portForwardPartsWithProtocol = 3
	portForwardPartsDefaultHTTP  = 2
	portBase                     = 10
	portBitSize                  = 16
)

// ErrInvalidTarget means the InstanceInfo cannot yield a usable backend
// (e.g. proxyGrpcAddress has no extractable IP).
var ErrInvalidTarget = errors.New("invalid route target")

// ComputeRoutes replicates the C++ TraefikCache::ParseRoutes computation:
// it turns one InstanceInfo into the set of (safeID, containerPort) -> backend
// targets implied by extensions["portForward"]. It does NOT consult instance
// status; the caller decides whether a RUNNING instance's routes are added or
// an exited instance's routes are removed (matching the C++ actor split).
//
// portForward is a JSON array of strings, each "protocol:hostPort:containerPort"
// (3 parts) or "hostPort:containerPort" (2 parts, http). The route key uses the
// containerPort (sandbox port); the target uses hostPort (node port). The host
// IP comes from proxyGrpcAddress.
//
// Valid inputs match the C++ output byte-for-byte. Malformed individual entries
// are skipped (slightly more lenient than the C++ try/catch, which aborts the
// remainder); a non-array / non-JSON portForward returns an error.
func ComputeRoutes(info *InstanceInfo) ([]*Target, error) {
	raw, ok := info.Extensions[portForwardKey]
	if !ok || raw == "" {
		return nil, nil // not a port-forwarding (sandbox) instance
	}

	hostIP := ExtractIP(info.ProxyGrpcAddress)
	if hostIP == "" {
		return nil, ErrInvalidTarget
	}
	safeID := SanitizeID(info.InstanceID)

	var entries []string
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, err
	}

	routes := make([]*Target, 0, len(entries))
	for _, e := range entries {
		parts := strings.Split(e, ":")
		var protocol, hostPort, containerPort string
		switch len(parts) {
		case portForwardPartsWithProtocol:
			protocol, hostPort, containerPort = parts[0], parts[1], parts[2]
		case portForwardPartsDefaultHTTP:
			protocol, hostPort, containerPort = "http", parts[0], parts[1]
		default:
			continue
		}

		cport, err := strconv.ParseUint(containerPort, portBase, portBitSize)
		if err != nil || cport == 0 {
			continue
		}
		hport, err := strconv.ParseUint(hostPort, portBase, portBitSize)
		if err != nil || hport == 0 {
			continue
		}

		scheme, ok := backendScheme(protocol)
		if !ok {
			continue
		}
		u := &url.URL{Scheme: scheme, Host: hostIP + ":" + strconv.FormatUint(hport, portBase)}
		routes = append(routes, &Target{
			InstanceID: info.InstanceID,
			RuntimeID:  info.RuntimeID,
			Key:        Key{SafeInstanceID: safeID, Port: uint16(cport)},
			TargetURL:  u,
			Scheme:     scheme,
		})
	}
	return routes, nil
}

func backendScheme(protocol string) (string, bool) {
	const canonicalRouteTokenCount = 2

	token := strings.ToLower(strings.TrimSpace(protocol))
	parts := strings.Split(token, "+")
	if len(parts) == 1 {
		switch parts[0] {
		case "http", "https":
			return parts[0], true
		case "tcp", "direct", "tunnel":
			// These are legacy transport/route tokens. Canonical mappings use
			// routeKind+backendScheme; the legacy backend defaults to HTTP.
			return "http", true
		default:
			return "", false
		}
	}
	if len(parts) != canonicalRouteTokenCount {
		return "", false
	}
	kind, scheme := parts[0], parts[1]
	if kind != "direct" && kind != "tunnel" && kind != "public" {
		return "", false
	}
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	return scheme, true
}
