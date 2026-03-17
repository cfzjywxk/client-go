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

import "time"

// IsolationLevel is the public transaction isolation level.
type IsolationLevel int

const (
	// IsolationSI uses snapshot isolation for reads.
	IsolationSI IsolationLevel = iota
	// IsolationRC uses read committed for reads.
	IsolationRC
)

// Priority is the public transaction priority.
type Priority int

const (
	// PriorityNormal is the default transaction priority.
	PriorityNormal Priority = iota
	// PriorityLow is a low priority transaction.
	PriorityLow
	// PriorityHigh is a high priority transaction.
	PriorityHigh
)

// TxnOptions configures Begin.
type TxnOptions struct {
	IsolationLevel IsolationLevel
	Pessimistic    bool
	ReadOnly       bool
	Timeout        time.Duration
	Priority       Priority
}

// SnapshotOptions configures GetSnapshot.
type SnapshotOptions struct {
	Realtime  bool
	StaleRead time.Duration
}

// TxnState is the externally visible state of a transaction handle.
type TxnState int

const (
	TxnStateActive TxnState = iota
	TxnStateCommitting
	TxnStateCommitted
	TxnStateRolledBack
	TxnStateAborted
)

// TxnHandle is an opaque transaction handle.
type TxnHandle interface {
	ID() uint64
	State() TxnState
	IsolationLevel() IsolationLevel
	Valid() bool
}
