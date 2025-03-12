package kvdb

import (
	"errors"
	"sync"

	"github.com/ongniud/wal"
)

// TxnState defines possible states of a transaction
type TxnState int

const (
	TxnInit TxnState = iota
	TxnActive
	TxnCommitted
	TxnAborted
)

// String representation of TransactionState (for debugging)
func (s TxnState) String() string {
	switch s {
	case TxnActive:
		return "Active"
	case TxnCommitted:
		return "Committed"
	case TxnAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// Transaction represents a transactional unit of work
type Transaction struct {
	ID      uint64
	wal     *wal.WAL
	store   *Store
	state   TxnState
	entries []*LogEntry
	mu      sync.Mutex
}

// NewTransaction initializes a transaction and writes a `LogBegin` entry
func NewTransaction(id uint64, wal *wal.WAL, store *Store) (*Transaction, error) {
	tx := &Transaction{
		ID:    id,
		wal:   wal,
		store: store,
		state: TxnInit,
	}
	return tx, nil
}

func (tx *Transaction) State() TxnState {
	return tx.state
}

// Begin writes the `LogBegin` record
func (tx *Transaction) Begin() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != TxnInit {
		return errors.New("transaction already started")
	}
	if err := tx.writeLog(&LogEntry{TxID: tx.ID, Type: LogBegin}); err != nil {
		return err
	}
	tx.state = TxnActive
	return nil
}

// Get ...
func (tx *Transaction) Get(key string) (string, bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.store.Get(key)
}

// Put adds an update operation to the transaction
func (tx *Transaction) Put(key, value string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != TxnActive {
		return errors.New("cannot write to a non-active transaction")
	}
	entry := &LogEntry{TxID: tx.ID, Type: LogUpdate, Key: key, Value: value}
	if err := tx.writeLog(entry); err != nil {
		return err
	}
	tx.entries = append(tx.entries, entry)
	return nil
}

// Delete ...
func (tx *Transaction) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != TxnActive {
		return errors.New("cannot write to a non-active transaction")
	}
	entry := &LogEntry{TxID: tx.ID, Type: LogDelete, Key: key}
	if err := tx.writeLog(entry); err != nil {
		return err
	}
	tx.entries = append(tx.entries, entry)
	return nil
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != TxnActive {
		return errors.New("transaction already committed or aborted")
	}
	// first
	if err := tx.writeLog(&LogEntry{TxID: tx.ID, Type: LogCommit}); err != nil {
		return err
	}
	if err := tx.wal.Sync(); err != nil {
		return err
	}
	// second
	for _, entry := range tx.entries {
		if entry.Type == LogUpdate {
			tx.store.Put(entry.Key, entry.Value)
		} else if entry.Type == LogDelete {
			tx.store.Delete(entry.Key)
		}
	}
	tx.state = TxnCommitted
	return tx.wal.Sync()
}

// Abort aborts the transaction
func (tx *Transaction) Abort() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != TxnActive {
		return errors.New("transaction already committed or aborted")
	}
	if err := tx.writeLog(&LogEntry{TxID: tx.ID, Type: LogAbort}); err != nil {
		return err
	}
	tx.state = TxnAborted
	return tx.wal.Sync()
}

// Helper method to write log entries
func (tx *Transaction) writeLog(entry *LogEntry) error {
	data, err := entry.Encode()
	if err != nil {
		return err
	}
	_, err = tx.wal.Write(data)
	return err
}
