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

// Package txnservice provides the Transaction Service API.
//
// This package defines the public interface for transaction operations,
// following the design principles from OceanBase's transaction service:
//   - TxnHandle is an opaque handle that hides internal state (startTS, commitTS, etc.)
//   - All transaction operations go through TxnService interface
//   - Internal 2PC protocol details are not exposed to consumers
//
// Example usage:
//
//	svc := txnservice.NewTxnService(store)
//	txn, err := svc.Begin(ctx, txnservice.TxnOptions{})
//	if err != nil {
//	    return err
//	}
//	defer svc.Rollback(ctx, txn) // Safe to call even after commit
//
//	val, err := svc.Get(ctx, txn, []byte("key"))
//	if err != nil {
//	    return err
//	}
//
//	if err := svc.Set(ctx, txn, []byte("key"), []byte("value")); err != nil {
//	    return err
//	}
//
//	return svc.Commit(ctx, txn)
package txnservice

import (
	"context"
)

// TxnService is the primary interface for transaction operations.
// It provides a clean API that hides internal implementation details
// such as timestamps, 2PC protocol, and storage layer interactions.
//
// Design principles (borrowed from OceanBase's ObTransService):
//   - Users cannot access or modify internal timestamps (startTS, commitTS)
//   - Transaction state is managed through state machine with strict transitions
//   - All operations require a TxnHandle obtained from Begin()
//
// Thread Safety: TxnService implementations must be safe for concurrent use.
// However, a single TxnHandle should not be used concurrently.
type TxnService interface {
	// ==================== Transaction Lifecycle ====================

	// Begin starts a new transaction and returns an opaque handle.
	// The handle must be used for all subsequent operations on this transaction.
	//
	// Internal details hidden from caller:
	//   - StartTS is obtained from TSO but not exposed
	//   - Transaction is registered with internal manager
	//
	// The returned TxnHandle is in Active state.
	Begin(ctx context.Context, opts TxnOptions) (TxnHandle, error)

	// Commit commits the transaction.
	// Internally executes 2PC protocol (prewrite + commit phases).
	//
	// After Commit returns:
	//   - On success: TxnHandle state becomes Committed
	//   - On failure: TxnHandle state becomes Aborted, caller should call Rollback
	//
	// Internal details hidden from caller:
	//   - CommitTS is obtained and used internally
	//   - Async commit / 1PC optimizations are applied automatically
	Commit(ctx context.Context, txn TxnHandle) error

	// Rollback aborts the transaction and releases all locks.
	// Safe to call multiple times or after successful commit (no-op in those cases).
	//
	// After Rollback returns, TxnHandle state becomes RolledBack.
	Rollback(ctx context.Context, txn TxnHandle) error

	// ==================== Data Operations ====================

	// Get retrieves the value for a single key within the transaction.
	// Returns (nil, nil) if key does not exist.
	//
	// For pessimistic transactions, this does NOT acquire a lock.
	// Use LockKeys() first if you need to lock before reading.
	Get(ctx context.Context, txn TxnHandle, key []byte) ([]byte, error)

	// BatchGet retrieves values for multiple keys within the transaction.
	// Keys that don't exist are omitted from the returned map.
	//
	// This is more efficient than multiple Get() calls.
	BatchGet(ctx context.Context, txn TxnHandle, keys [][]byte) (map[string][]byte, error)

	// Set writes a key-value pair within the transaction.
	// The write is buffered locally until Commit() is called.
	Set(ctx context.Context, txn TxnHandle, key, value []byte) error

	// Delete removes a key within the transaction.
	// The deletion is buffered locally until Commit() is called.
	Delete(ctx context.Context, txn TxnHandle, key []byte) error

	// Scan performs a range scan within the transaction.
	// Returns an iterator that yields key-value pairs in order.
	//
	// The iterator must be closed after use to release resources.
	Scan(ctx context.Context, txn TxnHandle, start, end []byte, limit int) (Iterator, error)

	// ==================== Pessimistic Locking ====================

	// LockKeys acquires pessimistic locks on the specified keys.
	// This is only valid for pessimistic transactions (TxnOptions.Pessimistic = true).
	//
	// Blocks until locks are acquired or timeout/conflict occurs.
	// Returns error if any key cannot be locked.
	LockKeys(ctx context.Context, txn TxnHandle, keys [][]byte) error

	// ==================== Savepoints ====================

	// CreateSavepoint creates a named savepoint within the transaction.
	// The savepoint captures the current state of buffered mutations.
	CreateSavepoint(ctx context.Context, txn TxnHandle, name string) error

	// RollbackToSavepoint rolls back the transaction to a named savepoint.
	// All mutations after the savepoint are discarded.
	// Pessimistic locks acquired after the savepoint are released.
	RollbackToSavepoint(ctx context.Context, txn TxnHandle, name string) error

	// ReleaseSavepoint releases a savepoint (optional cleanup).
	// This is a hint that the savepoint will not be used for rollback.
	ReleaseSavepoint(ctx context.Context, txn TxnHandle, name string) error
}

// SnapshotService provides read-only snapshot access without transaction overhead.
// Use this for pure read workloads that don't need transaction guarantees.
//
// Snapshots are lighter weight than full transactions:
//   - No 2PC overhead
//   - No lock acquisition
//   - Still provides consistent point-in-time view
type SnapshotService interface {
	// GetSnapshot obtains a read-only snapshot.
	// The snapshot provides a consistent view at a specific point in time.
	//
	// Internal details hidden from caller:
	//   - Snapshot timestamp is managed internally
	//   - Stale read optimization is applied based on options
	GetSnapshot(ctx context.Context, opts SnapshotOptions) (Snapshot, error)
}

// Snapshot provides read-only access to data at a consistent point in time.
// Obtained from SnapshotService.GetSnapshot().
//
// Thread Safety: Snapshot implementations should be safe for concurrent reads.
type Snapshot interface {
	// Get retrieves the value for a single key.
	// Returns (nil, nil) if key does not exist.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// BatchGet retrieves values for multiple keys.
	// Keys that don't exist are omitted from the returned map.
	BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error)

	// Scan performs a range scan.
	// Returns an iterator that yields key-value pairs in order.
	Scan(ctx context.Context, start, end []byte, limit int) (Iterator, error)

	// Valid returns whether this snapshot is still valid.
	// Snapshots may become invalid after a timeout or when explicitly released.
	Valid() bool

	// Release releases resources associated with this snapshot.
	// After Release(), the snapshot should not be used.
	Release()

	// NOTE: The following are deliberately NOT exposed:
	// - TS() uint64           // Internal timestamp not exposed
	// - Version() uint64      // Same as TS, not exposed
}

// Iterator provides sequential access to key-value pairs.
// Must be closed after use to release resources.
type Iterator interface {
	// Valid returns whether the iterator is positioned at a valid entry.
	Valid() bool

	// Key returns the key at the current position.
	// Only valid when Valid() returns true.
	Key() []byte

	// Value returns the value at the current position.
	// Only valid when Valid() returns true.
	Value() []byte

	// Next advances the iterator to the next entry.
	// Returns error if iteration fails.
	Next() error

	// Close releases resources held by the iterator.
	// Must be called when done with the iterator.
	Close()
}
