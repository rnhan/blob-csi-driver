/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blob

import (
	"context"
	"net"
	"strings"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cloud-provider-azure/pkg/metrics"
)

const (
	proxyHealthCheckInterval = 30 * time.Second
	proxyDialTimeout         = 3 * time.Second
)

// BlobfuseProxyMonitor continuously monitors blobfuse-proxy health.
type BlobfuseProxyMonitor struct {
	endpoint string
}

// NewBlobfuseProxyMonitor creates a new proxy health monitor.
func NewBlobfuseProxyMonitor(endpoint string) *BlobfuseProxyMonitor {
	return &BlobfuseProxyMonitor{
		endpoint: endpoint,
	}
}

// Start begins monitoring proxy health in background.
func (m *BlobfuseProxyMonitor) Start(ctx context.Context) {
	klog.V(2).Infof("Starting blobfuse-proxy health monitor for endpoint: %s", m.endpoint)

	ticker := time.NewTicker(proxyHealthCheckInterval)
	defer ticker.Stop()

	m.checkHealth()
	for {
		select {
		case <-ctx.Done():
			klog.V(2).Infof("Stopping blobfuse-proxy health monitor")
			return
		case <-ticker.C:
			m.checkHealth()
		}
	}
}

// checkHealth checks health and records metrics.
func (m *BlobfuseProxyMonitor) checkHealth() {
	mc := metrics.NewMetricContext(blobCSIDriverName, "blobfuse_proxy_health", "", "", "blobfuse-proxy")
	isOperationSucceeded := m.isProxyReachable()
	mc.ObserveOperationWithResult(isOperationSucceeded)
}

// isProxyReachable tests if it can connect to the proxy.
func (m *BlobfuseProxyMonitor) isProxyReachable() bool {
	network := "unix"
	address := strings.TrimPrefix(m.endpoint, "unix://")
	if !strings.HasPrefix(address, "/") {
		address = "/" + address
	}

	// Test actual connectivity to the Unix socket
	// Following the pattern used by containerd and other projects
	conn, err := net.DialTimeout(network, address, proxyDialTimeout)
	if err != nil {
		klog.V(6).Infof("Failed to dial blobfuse-proxy socket %s: %v", address, err)
		return false
	}
	conn.Close()

	klog.V(6).Infof("Successfully connected to blobfuse-proxy socket %s", address)
	return true
}
