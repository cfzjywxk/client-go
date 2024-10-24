// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"time"

	"github.com/tikv/client-go/v2/locate"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Storage represent the kv.Storage runs on TiKV.
type Storage interface {
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache

	// SendReq sends a request to TiKV.
	SendReq(bo *Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)

	// GetLockResolver gets the LockResolver.
	GetLockResolver() *LockResolver

	// GetSafePointKV gets the SafePointKV.
	GetSafePointKV() SafePointKV

	// UpdateSPCache updates the cache of safe point.
	UpdateSPCache(cachedSP uint64, cachedTime time.Time)

	// SetOracle sets the Oracle.
	SetOracle(oracle oracle.Oracle)

	// SetTiKVClient sets the TiKV client.
	SetTiKVClient(client Client)

	// GetTiKVClient gets the TiKV client.
	GetTiKVClient() Client

	// Closed returns the closed channel.
	Closed() <-chan struct{}

	// Close store
	Close() error
	// UUID return a unique ID which represents a Storage.
	UUID() string
	// CurrentTimestamp returns current timestamp with the given txnScope (local or global).
	CurrentTimestamp(txnScope string) (uint64, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
	// SupportDeleteRange gets the storage support delete range or not.
	SupportDeleteRange() (supported bool)
}
