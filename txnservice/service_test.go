// Copyright 2024 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnservice

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func newTestStore(t *testing.T) *tikv.KVStore {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestTxnServiceCRUDAndCommit(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store)
	ctx := context.Background()

	txn, err := svc.Begin(ctx, TxnOptions{})
	require.NoError(t, err)

	require.NoError(t, svc.Set(ctx, txn, []byte("k1"), []byte("v1")))
	got, err := svc.Get(ctx, txn, []byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)

	require.NoError(t, svc.Commit(ctx, txn))
	require.Equal(t, TxnStateCommitted, txn.State())

	snap, err := svc.GetSnapshot(ctx, SnapshotOptions{Realtime: true})
	require.NoError(t, err)
	defer snap.Release()

	snapVal, err := snap.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), snapVal)
}

func TestTxnServiceSavepointRollback(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store)
	ctx := context.Background()

	txn, err := svc.Begin(ctx, TxnOptions{})
	require.NoError(t, err)

	require.NoError(t, svc.Set(ctx, txn, []byte("base"), []byte("v0")))
	require.NoError(t, svc.CreateSavepoint(ctx, txn, "sp1"))
	require.NoError(t, svc.Set(ctx, txn, []byte("new"), []byte("v2")))

	require.NoError(t, svc.RollbackToSavepoint(ctx, txn, "sp1"))

	got, err := svc.Get(ctx, txn, []byte("base"))
	require.NoError(t, err)
	require.Equal(t, []byte("v0"), got)

	got, err = svc.Get(ctx, txn, []byte("new"))
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestTxnServiceReadOnlyTxn(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store)
	ctx := context.Background()

	txn, err := svc.Begin(ctx, TxnOptions{ReadOnly: true})
	require.NoError(t, err)

	err = svc.Set(ctx, txn, []byte("k1"), []byte("v1"))
	require.ErrorIs(t, err, ErrReadOnlyTxn)
}

func TestSnapshotServiceStaleRead(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store)
	ctx := context.Background()

	txn, err := svc.Begin(ctx, TxnOptions{})
	require.NoError(t, err)
	require.NoError(t, svc.Set(ctx, txn, []byte("k1"), []byte("v1")))
	require.NoError(t, svc.Commit(ctx, txn))
	time.Sleep(1100 * time.Millisecond)

	snap, err := svc.GetSnapshot(ctx, SnapshotOptions{StaleRead: time.Second})
	require.NoError(t, err)
	defer snap.Release()

	got, err := snap.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)
	require.True(t, snap.Valid())
	snap.Release()
	require.False(t, snap.Valid())
}

func TestTxnServiceScan(t *testing.T) {
	store := newTestStore(t)
	svc := NewService(store)
	ctx := context.Background()

	txn, err := svc.Begin(ctx, TxnOptions{})
	require.NoError(t, err)
	require.NoError(t, svc.Set(ctx, txn, []byte("a"), []byte("va")))
	require.NoError(t, svc.Set(ctx, txn, []byte("b"), []byte("vb")))
	require.NoError(t, svc.Set(ctx, txn, []byte("c"), []byte("vc")))

	iter, err := svc.Scan(ctx, txn, []byte("a"), []byte("d"), 2)
	require.NoError(t, err)
	defer iter.Close()

	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		require.NoError(t, iter.Next())
	}
	require.Equal(t, []string{"a", "b"}, keys)
}
