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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
)

var (
	// ErrInvalidTxnHandle indicates the provided handle was not created by this package.
	ErrInvalidTxnHandle = errors.New("invalid transaction handle")
	// ErrReadOnlyTxn indicates a write was attempted on a read-only transaction.
	ErrReadOnlyTxn = errors.New("transaction is read-only")
	// ErrLockRequiresPessimisticTxn indicates locking was requested for a non-pessimistic txn.
	ErrLockRequiresPessimisticTxn = errors.New("lock keys requires a pessimistic transaction")
	// ErrPipelinedSavepointUnsupported indicates savepoints are not implemented for pipelined txn.
	ErrPipelinedSavepointUnsupported = errors.New("savepoint is not supported for pipelined transaction")
)

// Service is a thin transaction-service wrapper on top of KVStore/KVTxn.
type Service struct {
	store  *tikv.KVStore
	nextID atomic.Uint64
}

// NewService creates a service that implements both TxnService and SnapshotService.
func NewService(store *tikv.KVStore) *Service {
	return &Service{store: store}
}

// NewTxnService creates a TxnService wrapper.
func NewTxnService(store *tikv.KVStore) TxnService {
	return NewService(store)
}

// NewSnapshotService creates a SnapshotService wrapper.
func NewSnapshotService(store *tikv.KVStore) SnapshotService {
	return NewService(store)
}

type txHandle struct {
	mu             sync.Mutex
	id             uint64
	state          TxnState
	isolationLevel IsolationLevel
	readOnly       bool
	pessimistic    bool
	timeout        time.Duration
	txn            *transaction.KVTxn
	savepoints     map[string]*savepoint
	order          []string
}

type savepoint struct {
	name       string
	checkpoint *unionstore.MemDBCheckpoint
}

func (h *txHandle) ID() uint64 {
	return h.id
}

func (h *txHandle) State() TxnState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

func (h *txHandle) IsolationLevel() IsolationLevel {
	return h.isolationLevel
}

func (h *txHandle) Valid() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state == TxnStateActive || h.state == TxnStateCommitting
}

func (s *Service) Begin(ctx context.Context, opts TxnOptions) (TxnHandle, error) {
	beginOpts := make([]tikv.TxnOption, 0, 1)
	txn, err := s.store.Begin(beginOpts...)
	if err != nil {
		return nil, err
	}
	applyTxnOptions(txn, opts)
	handle := &txHandle{
		id:             s.nextID.Add(1),
		state:          TxnStateActive,
		isolationLevel: normalizeIsolationLevel(opts.IsolationLevel),
		readOnly:       opts.ReadOnly,
		pessimistic:    opts.Pessimistic,
		timeout:        opts.Timeout,
		txn:            txn,
		savepoints:     make(map[string]*savepoint),
	}
	_ = ctx
	return handle, nil
}

func (s *Service) Commit(ctx context.Context, txn TxnHandle) error {
	handle, err := extractHandle(txn)
	if err != nil {
		return err
	}

	handle.mu.Lock()
	switch handle.state {
	case TxnStateCommitted, TxnStateRolledBack:
		handle.mu.Unlock()
		return nil
	case TxnStateAborted:
		handle.mu.Unlock()
		return errors.New("transaction has been aborted")
	case TxnStateCommitting:
		handle.mu.Unlock()
		return errors.New("transaction is already committing")
	case TxnStateActive:
		handle.state = TxnStateCommitting
	default:
		handle.mu.Unlock()
		return fmt.Errorf("unexpected transaction state %v", handle.state)
	}
	underlying := handle.txn
	handle.mu.Unlock()

	err = underlying.Commit(ctx)

	handle.mu.Lock()
	defer handle.mu.Unlock()
	if err != nil {
		handle.state = TxnStateAborted
		return err
	}
	handle.state = TxnStateCommitted
	return nil
}

func (s *Service) Rollback(_ context.Context, txn TxnHandle) error {
	handle, err := extractHandle(txn)
	if err != nil {
		return err
	}

	handle.mu.Lock()
	switch handle.state {
	case TxnStateCommitted, TxnStateRolledBack:
		handle.mu.Unlock()
		return nil
	case TxnStateAborted:
		handle.state = TxnStateRolledBack
		handle.mu.Unlock()
		return nil
	case TxnStateActive, TxnStateCommitting:
		underlying := handle.txn
		handle.mu.Unlock()
		err = underlying.Rollback()
		handle.mu.Lock()
		defer handle.mu.Unlock()
		if err != nil {
			handle.state = TxnStateAborted
			return err
		}
		handle.state = TxnStateRolledBack
		return nil
	default:
		handle.mu.Unlock()
		return fmt.Errorf("unexpected transaction state %v", handle.state)
	}
}

func (s *Service) Get(ctx context.Context, txn TxnHandle, key []byte) ([]byte, error) {
	handle, err := activeHandle(txn)
	if err != nil {
		return nil, err
	}
	entry, err := handle.txn.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if entry.IsValueEmpty() {
		return nil, nil
	}
	return entry.Value, nil
}

func (s *Service) BatchGet(ctx context.Context, txn TxnHandle, keys [][]byte) (map[string][]byte, error) {
	handle, err := activeHandle(txn)
	if err != nil {
		return nil, err
	}
	values, err := handle.txn.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(values))
	for key, entry := range values {
		if !entry.IsValueEmpty() {
			result[key] = entry.Value
		}
	}
	return result, nil
}

func (s *Service) Set(_ context.Context, txn TxnHandle, key, value []byte) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	if handle.readOnly {
		return ErrReadOnlyTxn
	}
	return handle.txn.Set(key, value)
}

func (s *Service) Delete(_ context.Context, txn TxnHandle, key []byte) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	if handle.readOnly {
		return ErrReadOnlyTxn
	}
	return handle.txn.Delete(key)
}

func (s *Service) Scan(_ context.Context, txn TxnHandle, start, end []byte, limit int) (Iterator, error) {
	handle, err := activeHandle(txn)
	if err != nil {
		return nil, err
	}
	iter, err := handle.txn.Iter(start, end)
	if err != nil {
		return nil, err
	}
	return newIterator(iter, limit), nil
}

func (s *Service) LockKeys(ctx context.Context, txn TxnHandle, keys [][]byte) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	if !handle.pessimistic {
		return ErrLockRequiresPessimisticTxn
	}
	wait := kv.LockAlwaysWait
	if handle.timeout > 0 {
		wait = int64(handle.timeout.Milliseconds())
	}
	lockCtx := kv.NewLockCtx(handle.txn.StartTS(), wait, time.Now())
	return handle.txn.LockKeys(ctx, lockCtx, keys...)
}

func (s *Service) CreateSavepoint(_ context.Context, txn TxnHandle, name string) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	if handle.txn.IsPipelined() {
		return ErrPipelinedSavepointUnsupported
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	handle.savepoints[name] = &savepoint{
		name:       name,
		checkpoint: handle.txn.GetMemBuffer().Checkpoint(),
	}
	handle.order = appendOrMoveToTail(handle.order, name)
	return nil
}

func (s *Service) RollbackToSavepoint(_ context.Context, txn TxnHandle, name string) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	if handle.txn.IsPipelined() {
		return ErrPipelinedSavepointUnsupported
	}
	handle.mu.Lock()
	savepoint, ok := handle.savepoints[name]
	if !ok {
		handle.mu.Unlock()
		return fmt.Errorf("savepoint %q not found", name)
	}
	handle.txn.GetMemBuffer().RevertToCheckpoint(savepoint.checkpoint)
	handle.order, handle.savepoints = truncateSavepoints(handle.order, handle.savepoints, name)
	handle.mu.Unlock()
	return nil
}

func (s *Service) ReleaseSavepoint(_ context.Context, txn TxnHandle, name string) error {
	handle, err := activeHandle(txn)
	if err != nil {
		return err
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if _, ok := handle.savepoints[name]; !ok {
		return fmt.Errorf("savepoint %q not found", name)
	}
	delete(handle.savepoints, name)
	handle.order = removeSavepoint(handle.order, name)
	return nil
}

func (s *Service) GetSnapshot(ctx context.Context, opts SnapshotOptions) (Snapshot, error) {
	ts, err := resolveSnapshotTS(ctx, s.store, opts)
	if err != nil {
		return nil, err
	}
	return &snapshotHandle{
		snapshot: s.store.GetSnapshot(ts),
		valid:    true,
	}, nil
}

type snapshotHandle struct {
	mu       sync.Mutex
	snapshot *txnsnapshot.KVSnapshot
	valid    bool
}

func (s *snapshotHandle) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid {
		return nil, errors.New("snapshot is not valid")
	}
	entry, err := s.snapshot.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if entry.IsValueEmpty() {
		return nil, nil
	}
	return entry.Value, nil
}

func (s *snapshotHandle) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid {
		return nil, errors.New("snapshot is not valid")
	}
	values, err := s.snapshot.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte, len(values))
	for key, entry := range values {
		if !entry.IsValueEmpty() {
			result[key] = entry.Value
		}
	}
	return result, nil
}

func (s *snapshotHandle) Scan(_ context.Context, start, end []byte, limit int) (Iterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid {
		return nil, errors.New("snapshot is not valid")
	}
	iter, err := s.snapshot.Iter(start, end)
	if err != nil {
		return nil, err
	}
	return newIterator(iter, limit), nil
}

func (s *snapshotHandle) Valid() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.valid
}

func (s *snapshotHandle) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.valid = false
	s.snapshot = nil
}

type iterator struct {
	base      unionstore.Iterator
	remaining int
	exhausted bool
}

func newIterator(base unionstore.Iterator, limit int) Iterator {
	return &iterator{
		base:      base,
		remaining: limit,
	}
}

func (it *iterator) Valid() bool {
	if it.exhausted || it.base == nil {
		return false
	}
	if it.remaining == 0 {
		return false
	}
	return it.base.Valid()
}

func (it *iterator) Key() []byte {
	return it.base.Key()
}

func (it *iterator) Value() []byte {
	return it.base.Value()
}

func (it *iterator) Next() error {
	if !it.Valid() {
		it.exhausted = true
		return nil
	}
	if it.remaining > 0 {
		it.remaining--
		if it.remaining == 0 {
			it.exhausted = true
			return nil
		}
	}
	return it.base.Next()
}

func (it *iterator) Close() {
	if it.base != nil {
		it.base.Close()
		it.base = nil
	}
	it.exhausted = true
}

func extractHandle(txn TxnHandle) (*txHandle, error) {
	handle, ok := txn.(*txHandle)
	if !ok || handle == nil || handle.txn == nil {
		return nil, ErrInvalidTxnHandle
	}
	return handle, nil
}

func activeHandle(txn TxnHandle) (*txHandle, error) {
	handle, err := extractHandle(txn)
	if err != nil {
		return nil, err
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.state != TxnStateActive {
		return nil, fmt.Errorf("transaction is not active: %v", handle.state)
	}
	return handle, nil
}

func normalizeIsolationLevel(level IsolationLevel) IsolationLevel {
	switch level {
	case IsolationRC:
		return IsolationRC
	default:
		return IsolationSI
	}
}

func applyTxnOptions(txn *transaction.KVTxn, opts TxnOptions) {
	switch normalizeIsolationLevel(opts.IsolationLevel) {
	case IsolationRC:
		txn.GetSnapshot().SetIsolationLevel(txnsnapshot.RC)
	default:
		txn.GetSnapshot().SetIsolationLevel(txnsnapshot.SI)
	}
	txn.SetPessimistic(opts.Pessimistic)
	switch opts.Priority {
	case PriorityLow:
		txn.SetPriority(txnutil.PriorityLow)
	case PriorityHigh:
		txn.SetPriority(txnutil.PriorityHigh)
	default:
		txn.SetPriority(txnutil.PriorityNormal)
	}
}

func resolveSnapshotTS(ctx context.Context, store *tikv.KVStore, opts SnapshotOptions) (uint64, error) {
	if opts.Realtime && opts.StaleRead > 0 {
		return 0, errors.New("Realtime and StaleRead cannot both be set")
	}
	if opts.StaleRead > 0 {
		prevSecond := uint64(opts.StaleRead / time.Second)
		if opts.StaleRead%time.Second != 0 {
			prevSecond++
		}
		if prevSecond == 0 {
			prevSecond = 1
		}
		ts, err := store.GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, prevSecond)
		if err != nil {
			return 0, err
		}
		err = store.GetOracle().ValidateReadTS(ctx, ts, true, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			return 0, err
		}
		return ts, nil
	}
	return store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
}

func appendOrMoveToTail(order []string, name string) []string {
	order = removeSavepoint(order, name)
	return append(order, name)
}

func removeSavepoint(order []string, name string) []string {
	result := order[:0]
	for _, item := range order {
		if item != name {
			result = append(result, item)
		}
	}
	return result
}

func truncateSavepoints(order []string, savepoints map[string]*savepoint, name string) ([]string, map[string]*savepoint) {
	for idx, item := range order {
		if item == name {
			for _, later := range order[idx+1:] {
				delete(savepoints, later)
			}
			return order[:idx+1], savepoints
		}
	}
	return order, savepoints
}
